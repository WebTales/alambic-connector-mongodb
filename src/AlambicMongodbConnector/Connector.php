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
        $db=!empty($baseConfig["db"]) ? $baseConfig["db"] : null;
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
            $start = !empty($payload['pipelineParams']['start']) ? $payload['pipelineParams']['start'] : null;
            $limit = !empty($payload['pipelineParams']['limit']) ? $payload['pipelineParams']['limit'] : null;
            $sort=null;
            if (!empty($payload['pipelineParams']['orderBy'])) {
                $direction = !empty($payload['pipelineParams']['orderByDirection']) && ($payload['pipelineParams']['orderByDirection'] == -'desc') ? -1 : 1;
                $sort=$payload['pipelineParams']['orderBy'];
            }
            $cursor = $collection->find($args);
            if($sort){
                $cursor->sort([$sort=>$direction]);
            }
            if($start){
                $cursor->skip($start);
            }
            if($limit){
                $cursor->limit($limit);
            }
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
        $args=isset($payload["args"]) ? $payload["args"] : [];
        if(empty($methodName)){
            throw new Exception('MongoDB connector requires a valid methodName for write ops');
        }
        if(empty($args['id'])&&$methodName!='create'){
            throw new Exception('MongoDB connector requires id for operations other than create');
        }
        $result=[];
        if(!empty($args['id'])){
            $args['_id']=new \MongoId($args['id']);
            unset($args['id']);
        }
        if($methodName=="create"){
            $collection->insert($args);
            $result=$args;
        } elseif ($methodName == "delete") {
            $delResult=$collection->remove($args);
            if(!isset($delResult["n"])||$delResult["n"]==0){
                throw new Exception('Found no record to delete');
            }
            $result=$args;
        } elseif ($methodName == "update") {
            $id= $args['_id'];
            unset($args['_id']);
            $result=$collection->findAndModify(["_id"=>$id],$args,null,["new"=>true]);
        }
        if(isset($result['_id'])){
            $result['id'] = (string)$result['_id'];
            unset($result['_id']);
        }
        $payload["response"]=$result;
        return $payload;
    }
}
