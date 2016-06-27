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
        if(!empty($args['id'])){
            $args['_id']=new \MongoId($args['id']);
            unset($args['id']);
        }
        $result=[];
        if(!$multivalued){
            $result=$collection->findOne($args);
            if(!empty($result['_id'])){
                $result['id']=(string) $result['_id'];
            }
        } else {
            $cursor = $collection->find($args);
            while ($cursor->hasNext()) {
                $item=$cursor->getNext();
                if(!empty($item['_id'])){
                    $item['id']=(string) $item['_id'];
                }
                $result[]=$item;
            }
        }
        $payload["response"]=$result;
        return $payload;
    }

    public function execute($payload=[],$collection){
        $methodName=isset($payload["methodName"]) ? $payload["methodName"] : null;
        if(empty($methodName)){
            throw new Exception('MongoDB connector requires a valid methodName for write ops');
        }
        if(empty($args['id'])&&$methodName!='creation'){
            throw new Exception('MongoDB connector requires id for operations other than create');
        }
        $result=[];
        if(!empty($args['id'])){
            $args['_id']=new \MongoId($args['id']);
            unset($args['id']);
        }
        if($methodName=="create"){
            $result=$collection->insert($args);
            if(isset($result['_id'])){
                $result['id'] = (string)$result['_id'];
                unset($result['_id']);
            }
        } elseif ($methodName == "delete") {
            $this->$collection->remove($args);
        } elseif ($methodName == "update") {
            $id= $args['_id'];
            unset($args['_id']);
            $result=$collection->findAndModify(["_id"=>$id],$args,null,["new"=>true]);
            if(isset($result['_id'])){
                $result['id'] = (string)$result['_id'];
                unset($result['_id']);
            }
        }
        $payload["response"]=$result;
        return $payload;
    }
}
