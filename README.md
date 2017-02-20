# Redis Extension for Flume NG

Redis extension for Flume NG ([http://flume.apache.org](http://flume.apache.org)). Tested with Apache Flume 1.5.0.1 and
Redis 3.2.8.

[![Build Status](https://travis-ci.org/chiwanpark/flume-ng-redis.png?branch=master)](https://travis-ci.org/chiwanpark/flume-ng-redis)
[![Coverage Status](https://coveralls.io/repos/chiwanpark/flume-ng-redis/badge.png)](https://coveralls.io/r/chiwanpark/flume-ng-redis)

## Current Version

* Development: 0.3.0-SNAPSHOT
* Stable: 0.2.0

## Current Supported Features

* Source using Redis [SUBSCRIBE](http://redis.io/commands/subscribe) command (multiple channels)
* Sink using Redis [PUBLISH](http://redis.io/commands/publish) command (only for single channel)

## Usage

1. Build or Download jar.
    * Checkout and build this repository.
        1. Stable release (currently version 0.2) is recommended.
        1. Build this library with ```mvn package -DskipTests``` command.
    * Or download built jar in release page.
      ([https://github.com/chiwanpark/flume-ng-redis/releases](https://github.com/chiwanpark/flume-ng-redis/releases))
1. Copy ```flume-ng-redis-[VERSION].jar``` or ```flume-ng-redis-[VERSION]-jar-with-dependencies.jar``` into your flume
   library path.
	* If you use ```flume-ng-redis-[VERSION].jar```, you have to download Jedis
	  ([https://github.com/xetorthio/jedis](https://github.com/xetorthio/jedis)) and copy it to flume library path.
1. Copy configuration sample file or create your own configuration.
1. Run Flume.
	* Following command is sample for RedisSubscribeDrivenSource.

			bin/flume-ng agent -n agent -c conf -f conf/example-SubscribeDrivenSource.properties -Dflume.root.logger=DEBUG,console
	
	* Following commend is sample for RedisPublishDrivenSink
	
			bin/flume-ng agent -n agent -c conf -f conf/example-PublishDrivenSink.properties

## Dependencies

* Jedis ([https://github.com/xetorthio/jedis](https://github.com/xetorthio/jedis))

## License

Copyright 2013-2015 Chiwan Park

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.