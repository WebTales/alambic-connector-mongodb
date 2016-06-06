<?php

namespace AlambicMongodbConnector;



use \Exception;

class Connector
{
    public function __invoke($payload=[])
    {
        $configs=isset($payload["configs"]) ? $payload["configs"] : [];
        $baseConfig=isset($payload["connectorBaseConfig"]) ? $payload["connectorBaseConfig"] : [];
        die("WIP");
    }

}
