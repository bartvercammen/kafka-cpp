/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
/*
 * producer.hpp
 *
 *  Created on: 21 Jun 2011
 *      Author: Ben Gray (@benjamg)
 */

#ifndef KAFKA_PRODUCER_HPP_
#define KAFKA_PRODUCER_HPP_

#include <string>
#include <deque>

#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <stdint.h>

#include "encoder.hpp"

namespace kafkaconnect {

const uint32_t use_random_partition = 0xFFFFFFFF;

class producer
{
public:
	producer();
	~producer();

	bool connect(const std::string& hostname, const uint16_t port);

	bool close();
	bool is_connected() const;
	bool is_connecting() const;

	bool send(std::string const& message, const std::string& topic, const uint32_t partition = kafkaconnect::use_random_partition)
	{
		boost::array<std::string, 1> messages = { { message } };
		return send(messages, topic, partition);
	}

	bool send(char const* message, const std::string& topic, const uint32_t partition = kafkaconnect::use_random_partition)
	{
		boost::array<std::string, 1> messages = { { message } };
		return send(messages, topic, partition);
	}

	template <typename List>
	bool send(const List& messages, const std::string& topic, const uint32_t partition = kafkaconnect::use_random_partition)
	{
		if (!is_connected())
		{
			return false;
		}

        std::stringstream stream;
        kafkaconnect::encode(stream, topic, partition, messages);
        write_impl(stream.str());

		return true;
	}

    unsigned long running_messages();


private:
	bool _connected;
    std::deque<std::string>         _outbox;
    int  _fd;

    void write_impl(const std::string& message);
    ssize_t _writen(int fd, const void *vptr, size_t n);
};

}

#endif /* KAFKA_PRODUCER_HPP_ */
