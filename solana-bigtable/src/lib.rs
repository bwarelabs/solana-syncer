use jni::objects::ReleaseMode;
use jni::objects::{JByteArray, JObject, JValue};
use jni::JNIEnv;

use serde::Deserialize;
use std::collections::HashMap;
use std::io::{stdout, Write};
use std::slice;

use prost::Message;
use solana_sdk::{
    clock::{Slot, UnixTimestamp},
    deserialize_utils::default_on_eof,
    message::v0::LoadedAddresses,
    pubkey::Pubkey,
    reserved_account_keys::ReservedAccountKeys,
    transaction::{TransactionError, VersionedTransaction},
};
use solana_storage_proto::convert::{entries, generated, tx_by_addr};
use solana_transaction_status::{
    extract_and_fmt_memos, ConfirmedBlock, ConfirmedTransactionStatusWithSignature,
    ConfirmedTransactionWithStatusMeta, EntrySummary, Reward, TransactionByAddrInfo,
    TransactionConfirmationStatus, TransactionStatus, TransactionStatusMeta,
    TransactionWithStatusMeta, VersionedConfirmedBlock, VersionedConfirmedBlockWithEntries,
    VersionedTransactionWithStatusMeta,
};

use crate::compression::{compress_best, decompress};

#[macro_use]
extern crate serde_derive;

mod compression;

fn slot_to_key(slot: Slot) -> String {
    format!("{slot:016x}")
}

fn slot_to_blocks_key(slot: Slot) -> String {
    slot_to_key(slot)
}

fn slot_to_entries_key(slot: Slot) -> String {
    slot_to_key(slot)
}

fn slot_to_tx_by_addr_key(slot: Slot) -> String {
    slot_to_key(!slot)
}

fn key_to_slot(key: &str) -> Option<Slot> {
    match Slot::from_str_radix(key, 16) {
        Ok(slot) => Some(slot),
        Err(err) => {
            // bucket data is probably corrupt
            //warn!("Failed to parse object key as a slot: {}: {}", key, err);
            None
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct StoredConfirmedBlockTransactionStatusMeta {
    err: Option<TransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
struct StoredConfirmedBlockTransaction {
    transaction: VersionedTransaction,
    meta: Option<StoredConfirmedBlockTransactionStatusMeta>,
}

#[derive(Serialize, Deserialize, Debug)]
struct StoredConfirmedBlockReward {
    pubkey: String,
    lamports: i64,
}

type StoredConfirmedBlockRewards = Vec<StoredConfirmedBlockReward>;

#[derive(Serialize, Deserialize, Debug)]
struct StoredConfirmedBlock {
    previous_blockhash: String,
    blockhash: String,
    parent_slot: Slot,
    transactions: Vec<StoredConfirmedBlockTransaction>,
    rewards: StoredConfirmedBlockRewards,
    block_time: Option<UnixTimestamp>,
    #[serde(deserialize_with = "default_on_eof")]
    block_height: Option<u64>,
}

// A serialized `TransactionInfo` is stored in the `tx` table
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
struct TransactionInfo {
    slot: Slot, // The slot that contains the block with this transaction in it
    index: u32, // Where the transaction is located in the block
    err: Option<TransactionError>, // None if the transaction executed successfully
    memo: Option<String>, // Transaction memo
}

impl From<StoredConfirmedBlock> for ConfirmedBlock {
    fn from(confirmed_block: StoredConfirmedBlock) -> Self {
        let StoredConfirmedBlock {
            previous_blockhash,
            blockhash,
            parent_slot,
            transactions,
            rewards,
            block_time,
            block_height,
        } = confirmed_block;

        Self {
            previous_blockhash,
            blockhash,
            parent_slot,
            transactions: transactions.into_iter().map(|tx| tx.into()).collect(),
            rewards: rewards.into_iter().map(|reward| reward.into()).collect(),
            num_partitions: None,
            block_time,
            block_height,
        }
    }
}

impl From<StoredConfirmedBlockTransaction> for TransactionWithStatusMeta {
    fn from(tx_with_meta: StoredConfirmedBlockTransaction) -> Self {
        let StoredConfirmedBlockTransaction { transaction, meta } = tx_with_meta;
        match meta {
            None => Self::MissingMetadata(
                transaction
                    .into_legacy_transaction()
                    .expect("versioned transactions always have meta"),
            ),
            Some(meta) => Self::Complete(VersionedTransactionWithStatusMeta {
                transaction,
                meta: meta.into(),
            }),
        }
    }
}

impl From<StoredConfirmedBlockTransactionStatusMeta> for TransactionStatusMeta {
    fn from(value: StoredConfirmedBlockTransactionStatusMeta) -> Self {
        let StoredConfirmedBlockTransactionStatusMeta {
            err,
            fee,
            pre_balances,
            post_balances,
        } = value;
        let status = match &err {
            None => Ok(()),
            Some(err) => Err(err.clone()),
        };
        Self {
            status,
            fee,
            pre_balances,
            post_balances,
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: None,
            post_token_balances: None,
            rewards: None,
            loaded_addresses: LoadedAddresses::default(),
            return_data: None,
            compute_units_consumed: None,
        }
    }
}

impl From<StoredConfirmedBlockReward> for Reward {
    fn from(value: StoredConfirmedBlockReward) -> Self {
        let StoredConfirmedBlockReward { pubkey, lamports } = value;
        Self {
            pubkey,
            lamports,
            post_balance: 0,
            reward_type: None,
            commission: None,
        }
    }
}

pub fn generate_upload_vectors(
    env: &mut JNIEnv,
    object: &JObject,
    slot: Slot,
    confirmed_block: ConfirmedBlock,
) {
    let confirmed_block: VersionedConfirmedBlock = match confirmed_block.try_into() {
        Ok(confirmed_block) => confirmed_block,
        Err(_) => {
            return;
        }
    };

    let txs_java_array: JObject = env
        .get_field(&object, "txs", "Ljava/util/List;")
        .unwrap()
        .l()
        .unwrap();

    let mut by_addr: HashMap<&Pubkey, Vec<TransactionByAddrInfo>> = HashMap::new();
    let reserved_account_keys = ReservedAccountKeys::new_all_activated();
    for (index, transaction_with_meta) in confirmed_block.transactions.iter().enumerate() {
        let VersionedTransactionWithStatusMeta { meta, transaction } = transaction_with_meta;
        let err = meta.status.clone().err();
        let index = index as u32;
        let signature = transaction.signatures[0];
        let memo = extract_and_fmt_memos(transaction_with_meta);

        for address in transaction_with_meta.account_keys().iter() {
            // Historical note that previously only a set of sysvar ids were
            // skipped from being uploaded. Now we skip uploaded for the set
            // of all reserved account keys which will continue to grow in
            // the future.
            if !reserved_account_keys.is_reserved(address) {
                by_addr
                    .entry(address)
                    .or_default()
                    .push(TransactionByAddrInfo {
                        signature,
                        err: err.clone(),
                        index,
                        memo: memo.clone(),
                        block_time: confirmed_block.block_time,
                    });
            }
        }

        let java_key = env.new_string(&signature.to_string()).unwrap();
        let java_value = env
            .byte_array_from_slice(
                &compress_best(
                    &bincode::serialize(&TransactionInfo {
                        slot,
                        index,
                        err,
                        memo,
                    })
                    .unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
        let cell = env
            .new_object(
                "com/bwarelabs/BigtableCell",
                "(Ljava/lang/String;[B)V",
                &[(&java_key).into(), (&java_value).into()],
            )
            .unwrap();
        env.call_method(
            &txs_java_array,
            "add",
            "(Ljava/lang/Object;)Z",
            &[(&cell).into()],
        )
        .unwrap();
    }

    let tx_by_addrs_java_array: JObject = env
        .get_field(&object, "txByAddrs", "Ljava/util/List;")
        .unwrap()
        .l()
        .unwrap();
    by_addr.into_iter().for_each(|(address, transaction_info_by_addr)| {
        let java_key = env
            .new_string(format!("{}/{}", address, slot_to_tx_by_addr_key(slot)))
            .unwrap();
        let java_value = env
            .byte_array_from_slice(
                &compress_best(
                    &(tx_by_addr::TransactionByAddr {
                        tx_by_addrs: transaction_info_by_addr.into_iter().map(Into::into).collect(),
                    })
                    .encode_to_vec(),
                )
                .unwrap(),
            )
            .unwrap();
        let cell = env
            .new_object(
                "com/bwarelabs/BigtableCell",
                "(Ljava/lang/String;[B)V",
                &[(&java_key).into(), (&java_value).into()],
            )
            .unwrap();
        env.call_method(
            &tx_by_addrs_java_array,
            "add",
            "(Ljava/lang/Object;)Z",
            &[(&cell).into()],
        )
        .unwrap();
    });
}

#[no_mangle]
pub extern "system" fn Java_com_bwarelabs_BigtableBlock_process<'local>(
    mut env: JNIEnv<'local>,
    object: JObject<'local>,
) {
    let block_type = env.get_field(&object, "type", "I").unwrap().i().unwrap();
    let slot: Slot = env.get_field(&object, "slot", "J").unwrap().j().unwrap() as u64;
    let compressed_block: JByteArray = env
        .get_field(&object, "compressedBlock", "[B")
        .unwrap()
        .l()
        .unwrap()
        .into();
    let data = unsafe {
        let java_array = env
            .get_array_elements_critical(&compressed_block, ReleaseMode::NoCopyBack)
            .unwrap();
        decompress(slice::from_raw_parts(
            java_array.as_ptr() as *mut u8,
            java_array.len(),
        ))
        .unwrap()
    };

    if block_type == 0 {
        // protobuf
        let block = generated::ConfirmedBlock::decode(&data[..]).unwrap();
        let confirmed_block: ConfirmedBlock = block.try_into().unwrap();
        generate_upload_vectors(&mut env, &object, slot, confirmed_block);
    } else {
        // bincode
        let block: StoredConfirmedBlock = bincode::deserialize(&data).unwrap();
        let confirmed_block: ConfirmedBlock = block.into();
        generate_upload_vectors(&mut env, &object, slot, confirmed_block);
    };
}
