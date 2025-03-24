TODO

poate ar trb scris cum trb date cheile, in hex, in decimal? si check daca e posibil


when starting the hbase instance, you must mount the path where you want to export the sequencefiles otherwise you will lose the data from the hbase because you can t add the volume later (use docker commit to save the day in case you missed this note :D)

idealy, should mount the export_from_hbase.sh script too in the hbase container

TODO: when creating tables for hbase, make them have just one region and increase the file size: 
hbase shell
hbase> create 'blocks_copy', {NAME => 'x'}, CONFIGURATION => {'hbase.hregion.max.filesize' => '1099511627776', 'hbase.hregion.split.overallfiles' => 'true'}

check if there are multiple regions after populating the table

steps:
- create /data/recovery folder
- create a sequencefile folder in solana-syncer/recovery/tencent-upload and mount it to the hbase docker container (this is where the files will be exported)
- start the docker HBase container and set the HBASE_HOST in ./hbase-import/Dockerfile with docker's ip
- start the ./start.sh script


-- make a /data/recovery folder
-- make a /data/hbase folder and make ubuntu owner so that hbase can write in it