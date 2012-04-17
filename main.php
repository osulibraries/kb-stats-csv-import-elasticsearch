<?php

$ELASTIC_HOST = 'es-dev.lib.ohio-state.edu';
$ELASTIC_PORT = 9200;
$ELASTIC_INDEX = 'kb';
$ELASTIC_TYPE = 'stats';

$RELATIVE_PATH_TO_LOGS = '/../logs/';

define('ELASTICA_LIBRARY_PATH', dirname(__FILE__) . '/library/Elastica/lib/');
define('CSV_LOG_PATH', dirname(__FILE__) . $RELATIVE_PATH_TO_LOGS);


function __autoload_elastica($class)
{
    $path = str_replace('_', '/', $class);

    if (file_exists(ELASTICA_LIBRARY_PATH . $path . '.php')) {
        require_once(ELASTICA_LIBRARY_PATH . $path . '.php');
    }
}

spl_autoload_register('__autoload_elastica');


require_once(dirname(__FILE__) . '/library/ParseCsv.php');


$elasticaClient = new Elastica_Client(array(
    'host'=>$ELASTIC_HOST,
    'port'=>$ELASTIC_PORT
));


$index = $elasticaClient->getIndex($ELASTIC_INDEX);


$stats = $index->getType($ELASTIC_TYPE);


$mapping = new Elastica_Type_Mapping($stats);


$propertiesJSON = <<<JSON
{
   "userAgent":{
      "type":"string"
   },
   "countryCode":{
      "type":"string",
      "index":"not_analyzed",
      "omit_norms":true
   },
   "dns":{
      "type":"multi_field",
      "fields": {
        "dns": {"type":"string","index":"analyzed"},
        "untouched":{"type":"string","index":"not_analyzed"}
      }
   },
   "isBot":{
      "type":"boolean"
   },
   "owningColl":{
      "type":"integer",
      "index":"not_analyzed"
   },
   "type":{
      "type":"string",
      "index":"not_analyzed",
      "omit_norms":true
   },
   "owningComm":{
      "type":"integer",
      "index":"not_analyzed"
   },
   "city":{
      "type":"multi_field",
      "fields": {
        "city": {"type":"string","index":"analyzed"},
        "untouched":{"type":"string","index":"not_analyzed"}
      }
   },
   "country":{
      "type":"multi_field",
      "fields": {
         "country": {"type":"string","index":"analyzed"},
         "untouched":{"type":"string","index":"not_analyzed"}
       }
   },
   "ip":{
      "type":"multi_field",
       "fields": {
         "ip": {"type":"string","index":"analyzed"},
         "untouched":{"type":"string","index":"not_analyzed"}
       }
   },
   "id":{
      "type":"integer",
      "index":"not_analyzed"
   },
   "time":{
      "type":"date"
   },
   "owningItem":{
      "type":"string",
      "index":"not_analyzed"
   },
   "continent":{
      "type":"string",
      "index":"not_analyzed"
   },
   "geo":{
      "type":"geo_point"
   },
   "bundleName":{
      "type":"string",
      "index":"not_analyzed"
   },
   "epersonid":{
      "type":"string",
      "index":"not_analyzed"
   }
}
JSON;

$mapping->setProperties( json_decode($propertiesJSON,true) );
echo "Setting Elastic Schema Properties...\n";
$mapping->send();

echo "ImportUsage: php main.php /path/to/csvlogs\n";
$parser = new ParseCsv(CSV_LOG_PATH);


$multiples = array(
    'owningColl',
    'owningComm'
);

$i=0;

foreach ($parser as $docs) {

    echo "Starting new csv doc (doc count = $i )\n";

    $batchDocs = array();
    $startTime=microtime(true);
    $startDocs = $i;


    foreach ($docs as $doc) {
        $i++;

        foreach($multiples as $col){
            if(strpos($doc[$col],',') > -1){
                $doc[$col] = explode(',',$doc[$col]);
                array_unique($doc[$col]);
                foreach($doc[$col] as &$num){
                    $num = intval($num);
                }

            }
            else{
                $doc[$col] = intval($doc[$col]);
            }
        }

        $time = strtotime($doc['time']);

        $doc['time'] = date(DATE_ISO8601, $time);

         //gmdate("Y-m-d\TH:i:s\Z",$time);

        if($doc['latitude'] != '' && $doc['longitude'] != '') {
            $doc['geo'] = array(
                'lat'=>$doc['latitude'],
                'lon'=>$doc['longitude']
            );
        }

        unset($doc['latitude'],$doc['longitude']);

        $doc['reverseDns'] = trim(implode('.',array_reverse(explode('.',$doc['dns']))),'.');

        if($doc['type']==0) {
            $doc['type']="bitstream";
        } else if($doc['type']==2) {
            $doc['type']="item";
        } else if($doc['type']==3) {
            $doc['type']="collection";
        } else if($doc['type']==4) {
            $doc['type']="community";
        } else {
            $doc['type']="unknown";
        }

        $doc = new Elastica_Document(null, $doc);
        $batchDocs[] = $doc;


        //$stats->addDocument($doc);


    }


    $stats->addDocuments($batchDocs);
    $endTime=microtime(true);
    $endDocs = $i;

    echo 'Added '.($endDocs-$startDocs).' docs in '.($endTime-$startTime).' time, thus '.(($endDocs-$startDocs)/($endTime-$startTime)).' is some perf score.';


}




