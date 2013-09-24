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
#include <iostream>

#include "producer.hpp"

namespace kafkaconnect {

using std::endl;
using std::cout;

producer::producer()
	: _connected(false)
    , _outbox()
    , _fd(0)
{ }

producer::~producer()
{ }

bool producer::connect(const std::string& hostname, const uint16_t port)
{
	_connected=false;
    if ((_fd = socket(PF_INET, SOCK_STREAM, 0)) == -1)
    {
    	cout << "can't connect to kafka server" << endl;
    	return false;
    }

    cout << "Trying to connect to " << hostname << ":" << port << endl;;
    struct sockaddr_in addr;
    bzero(&addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (1 != (inet_pton(AF_INET, hostname.c_str(), &addr.sin_addr)))
    {
        cout << "illegal server address " << hostname << ":" << port << endl;;
        return false;
    }

    if (0 !=::connect(_fd, (struct sockaddr *)&addr, sizeof(addr)))
    {
        cout << "connect error to " << hostname << ":" << port << " error " << strerror(errno) << endl;;
        return false;
    }
    
    cout << "connected to " <<hostname << ":" <<port << endl;;
    _connected=true;
    return true;
}

bool producer::close()
{
	_connected = false;
	::close(_fd);
    _fd = 0;
}

bool producer::is_connected() const
{
	return _connected;
}

//this method writes exactly n bytes to a socket
ssize_t producer::_writen(int fd, const void *vptr, size_t n)
{
    size_t nleft;
    ssize_t nwritten;
    const char *ptr;

    ptr = (char*)vptr;
    nleft = n;
    while (nleft > 0) {
        if ( (nwritten = ::send(fd, ptr, nleft,MSG_NOSIGNAL)) <= 0) { // prevent SIGPIPE if socket is closed
            if (nwritten < 0 && errno == EINTR)
                nwritten = 0;   /* and call write() again */
            else
                return (-1);    /* error */
        }
        nleft -= nwritten;
        ptr += nwritten;
    }
    return (n);
}

void
producer::write_impl(const std::string& data)
{
	int n=_writen(_fd,data.c_str(),data.size());
    if(n<=0)
    {
    	cout << "can't write to KAFKA server - error "<< strerror(errno) << endl;
        // try to reconnect
        _connected=false;
        ::close(_fd);
      // send data to kafka (is old data but just send it, do this only once )
      if(_writen(_fd,data.c_str(),data.size())<=0) // just in case connection is closed again
      {
    	  cout << "can't write to KAFKA server - error "<< strerror(errno) << endl;
    	  _connected=false;
    	  ::close(_fd);
      }
    }
}

}
