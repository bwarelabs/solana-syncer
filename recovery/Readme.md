TODO

poate ar trb scris cum trb date cheile, in hex, in decimal? si check daca e posibil



when starting the hbase instance, you must mount the path where you want to export the sequencefiles otherwise you will lose the data from the hbase because you can t add the volume later (use docker commit to save the day in case you missed this note :D)

idealy, should mount the export_from_hbase.sh script too in the hbase container

TODO: when creating tables for hbase, make them have just one region and increase the file size: 
hbase shell
hbase> create 'blocks_copy', {NAME => 'x'}, CONFIGURATION => {'hbase.hregion.max.filesize' => '1099511627776', 'hbase.hregion.split.overallfiles' => 'true'}

check if there are multiple regions after populating the table