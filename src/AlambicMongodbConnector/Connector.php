<?php

namespace AlambicMongodbConnector;



use \Exception;

class Connector
{
    public function __invoke($payload=[])
    {
        if (isset($payload["response"])) {
            return $payload;
        }
        $configs=isset($payload["configs"]) ? $payload["configs"] : [];
        $baseConfig=isset($payload["connectorBaseConfig"]) ? $payload["connectorBaseConfig"] : [];
        $db=!empty($configs["db"]) ? $baseConfig["db"] : null;
        if (!empty($configs["db"])){
            $db=$configs["db"];
        }
        if (!$db){
            throw new Exception('Insufficient configuration : db required');
        }
        if (empty($configs["collection"])){
            throw new Exception('Insufficient configuration : collection required');
        }
        $connectionOptions=!empty($baseConfig["connectionOptions"]) ? $baseConfig["connectionOptions"] : [];
        $client= !empty($baseConfig["connectionString"]) ? new \MongoClient($baseConfig["connectionString"]) : new \MongoClient();
        $database=$client->{$db};
        $collection=$database->{$configs["collection"]};
        return $payload["isMutation"] ? $this->execute($payload,$collection) : $this->resolve($payload,$collection);
    }

    public function resolve($payload=[],$collection){
        $multivalued=isset($payload["multivalued"]) ? $payload["multivalued"] : false;
        $args=isset($payload["args"]) ? $payload["args"] : [];
        if(!empty($args['_id'])){
            $args['_id']=new \MongoId($args['_id']);
        }
        $result=[];
        if(!$multivalued){
            $result=$collection->findOne($args);
            if(!empty($result['_id'])){
                $result['_id']=(string) $result['_id'];
            }
        } else {
            $cursor = $collection->find($args);
            while ($cursor->hasNext()) {
                $item=$cursor->getNext();
                if(!empty($item['_id'])){
                    $item['_id']=(string) $item['_id'];
                }
                $result[]=$item;
            }
        }
        $payload["response"]=$result;
        return $payload;
    }

    public function execute($payload=[],$collection){
        //WIP
        $payload["response"]=[];
        return $payload;
    }
}
