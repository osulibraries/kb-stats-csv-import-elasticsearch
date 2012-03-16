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
      "type":"integer",
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
      "format":"date_time_no_millis",
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

$mapping->send();


$parser = new ParseCsv(CSV_LOG_PATH);


$multiples = array(
    'owningColl',
    'owningComm'
);

foreach ($parser as $docs) {

    echo "Starting new csv doc\n";



    foreach ($docs as $doc) {


        foreach($multiples as $col){
            if(strpos($doc[$col],',') > -1){
                $doc[$col] = explode(',',$doc[$col]);
                array_unique($doc[$col]);
                foreach($doc[$col] as &$num){
                    $num = intval($num);
                }

            }
            else{
                $doc[$col] = intval($doc['col']);
            }
        }

        $time = strtotime($doc['time']);

        $doc['time']= gmdate("Y-m-d\TH:i:s\Z",$time);

        $doc['geo'] = array(
            'lat'=>$doc['latitude'],
            'lon'=>$doc['longitude']
        );
        unset($doc['latitude'],$doc['longitude']);

        $doc['reverseDns'] = trim(implode('.',array_reverse(explode('.',$doc['dns']))),'.');


        $doc = new Elastica_Document(null, $doc);


        $stats->addDocument($doc);


    }



    //$stats->addDocuments($docs);


}




