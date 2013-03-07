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
 * producer.cpp
 *
 *  Created on: 21 Jun 2011
 *      Author: Ben Gray (@benjamg)
 */

#include <boost/lexical_cast.hpp>
#include <sys/sysinfo.h>

#include "producer.hpp"

namespace kafkaconnect {

producer::producer( boost::asio::io_service&        io_service, 
                    unsigned int                    queue_memory_percentage,
                    const error_handler_function&   error_handler)
	: _connected(false)
	, _connecting(false)
	, _resolver(io_service)
	, _socket(io_service)
	, _error_handler(error_handler)
    , _strand(io_service)
    , _outbox()
    , _mem_treshhold(queue_memory_percentage)
    , _mem_bytes_usable(getTotalSystemMemory())
    , _mem_bytes_used(0)
{ }

producer::producer(boost::asio::io_service& io_service, const error_handler_function& error_handler)
	: _connected(false)
	, _connecting(false)
	, _resolver(io_service)
	, _socket(io_service)
	, _error_handler(error_handler)
    , _strand(io_service)
    , _outbox()
    , _mem_treshhold(0)
    , _mem_bytes_usable(0)
    , _mem_bytes_used(0)
{ }

producer::~producer()
{
	close();
}

bool producer::connect(const std::string& hostname, const uint16_t port)
{
	return connect(hostname, boost::lexical_cast<std::string>(port));
}

bool producer::connect(const std::string& hostname, const std::string& servicename)
{
	if (_connecting) { return false; }
	_connecting = true;

	boost::asio::ip::tcp::resolver::query query(hostname, servicename);
	_resolver.async_resolve(
		query,
		boost::bind(
			&producer::handle_resolve, this,
			boost::asio::placeholders::error, boost::asio::placeholders::iterator
		)
	);
}

bool producer::close()
{
	if (_connecting) { return false; }

	_connected = false;
	_socket.close();
}

bool producer::is_connected() const
{
	return _connected;
}

bool producer::is_connecting() const
{
	return _connecting;
}

void producer::handle_resolve(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints)
{
	if (!error_code)
	{
		boost::asio::ip::tcp::endpoint endpoint = *endpoints;
		_socket.async_connect(
			endpoint,
			boost::bind(
				&producer::handle_connect, this,
				boost::asio::placeholders::error, ++endpoints
			)
		);
	}
	else
	{
		_connecting = false;
		fail_fast_error_handler(error_code);
	}
}

void producer::handle_connect(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints)
{
	if (!error_code)
	{
		// The connection was successful.
		_connecting = false;
		_connected = true;
	}
	else if (endpoints != boost::asio::ip::tcp::resolver::iterator())
	{
		// TODO: handle connection error (we might not need this as we have others though?)

		// The connection failed, but we have more potential endpoints so throw it back to handle resolve
		_socket.close();
		handle_resolve(boost::system::error_code(), endpoints);
	}
	else
	{
		_connecting = false;
		fail_fast_error_handler(error_code);
	}
}

void producer::handle_write_request(const boost::system::error_code& error_code)
{
    _mem_bytes_used -= _outbox.front().length();
    _outbox.pop_front();

	if (error_code)	{
		fail_fast_error_handler(error_code);
	}

    if(!_outbox.empty()) {
        this->write();
    }
}

void
producer::write_impl(const std::string& message)
{
    _outbox.push_back(message);
    _mem_bytes_used += message.length();

    if(_outbox.size() > 1) {
        return;
    }

    this->write();
}

void
producer::write()
{
    boost::asio::async_write( _socket,
                              boost::asio::buffer( _outbox[0].data(), _outbox[0].size() ),
                              _strand.wrap( boost::bind( &producer::handle_write_request,
                                                         this,
                                                         boost::asio::placeholders::error )));
}

unsigned long
producer::running_messages()
{
    return _outbox.size();
}

unsigned long 
producer::getTotalSystemMemory() const
{
    struct sysinfo info;
    sysinfo( &info );
    return (unsigned long)info.totalram * (unsigned long)info.mem_unit;
}

bool
producer::hasMemoryLeft(unsigned int size) const
{
/*
    std::cout << "memory: used=" << _mem_bytes_used
              << ", usable=" << _mem_bytes_usable
              << ", treshold=" << _mem_treshhold
              << " (=" << ((_mem_bytes_usable * _mem_treshhold) / 100) << " bytes)" << std::endl;
*/
    return ((_mem_bytes_usable * _mem_treshhold) / 100) > (_mem_bytes_used + size);
}

}

