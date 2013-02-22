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
	typedef boost::function<void(boost::system::error_code const&)> error_handler_function;

	producer(boost::asio::io_service& io_service, const error_handler_function& error_handler = error_handler_function());
	~producer();

	bool connect(const std::string& hostname, const uint16_t port);
	bool connect(const std::string& hostname, const std::string& servicename);

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
        _strand.post( boost::bind( &producer::write_impl,
                                   this,
                                   stream.str() ) );

		return true;
	}

    unsigned long running_messages();


private:
	bool _connected;
	bool _connecting;
	boost::asio::ip::tcp::resolver  _resolver;
	boost::asio::ip::tcp::socket    _socket;
	error_handler_function          _error_handler;
    boost::asio::io_service::strand _strand;
    std::deque<std::string>         _outbox;

    void write_impl(const std::string& message);
    void write();

	void handle_resolve(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints);
	void handle_connect(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints);
	void handle_write_request(const boost::system::error_code& error_code);

	/* Fail Fast Error Handler Braindump
	 *
	 * If an error handler is not provided in the constructor then the default response is to throw
	 * back the boost error_code from asio as a boost system_error exception.
	 *
	 * Most likely this will cause whatever thread you have processing boost io to terminate unless caught.
	 * This is great on debug systems or anything where you use io polling to process any outstanding io,
	 * however if your io thread is seperate and not monitored it is recommended to pass a handler to
	 * the constructor.
	 */
	inline void fail_fast_error_handler(const boost::system::error_code& error_code)
	{
		if(_error_handler.empty()) { throw boost::system::system_error(error_code); }
		else { _error_handler(error_code); }
	}
};

}

#endif /* KAFKA_PRODUCER_HPP_ */
