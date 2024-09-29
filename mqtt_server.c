/*
 * File: mqtt_server.c
 * Project: simple-mqtt-server
 * File Created: Saturday, 28th Sept 2024
 * Author: Ravi Verman (raviverman@hotmail.com)
 * -----
 * Last Modified: Sunday, 29th Sept 2024
 * Modified By: Ravi Verman (raviverman@hotmail.com)
 * -----
 * SPDX-License-Identifier: GPL-2.0-only
 * Copyright 2024 Ravi Verman
 */
#include<sys/socket.h>
#include<netinet/in.h>
#include<unistd.h>
#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<fcntl.h>
#include<assert.h>
#include<pthread.h>
#include<stdbool.h>
#include<sys/epoll.h>
#include<arpa/inet.h>
#include<time.h>

#define PORT 1883
#define BUFFER_SIZE 2048
#define MQTT_MSG_MAX_LEN 256
#define QUEUE_LEN_MAX 25
#define CONST_TIMESTAMP 192222250
#define DEVICE_ID "c0defade"

#define MSG_TYPE(value) (message_type[(value & 0xf)]->name)
#define SET_TYPE(type) { type, #type }
#define BYTE2_INT(value) (((value)[0]) << 8 | ((value)[1]))
#define IS_REQUEST(req_and_flag, request_type) \
((((req_and_flag) >> 4) & 0xf) == request_type) 

#define INT_TO_BYTE2(int_value, char2) do {  \
(char2)[1] = int_value & 0xff;               \
(char2)[0] = (int_value >> 8) & 0xff;        \
} while(0);                                  \

int trace_fd = 0;
char *trace_file = "/tmp/mqtt_server.log";

#define DEBUG printf
// TODO: Add log levels
#define TRACE(...) do {                                \
char printbuffer[2048];                                \
time_t mytime = time(NULL);                            \
char *time_str = ctime(&mytime);                       \
int cnt = 0;                                           \
cnt = snprintf(printbuffer, 2048, "> %s", time_str);   \
write(trace_fd, printbuffer, cnt);                     \
cnt = snprintf(printbuffer, 256, __VA_ARGS__ );        \
write(trace_fd, printbuffer, cnt);                     \
} while(0);

#define INIT_TRACE_TO_BUFFER   \
char print_buffer[2048];       \
int print_buffer_index = 0;    \

#define TRACE_TO_BUFFER(...) do {                     \
print_buffer_index += snprintf(&print_buffer[print_buffer_index], 256, __VA_ARGS__ );   \
assert( print_buffer_index < 2048 && "Increase print buffer size" ); \
} while(0);

#define TRACE_TO_BUFFER_FLUSH    \
TRACE(print_buffer);

void
init_tf() {
   int f = open(trace_file, O_RDWR | O_CREAT | O_TRUNC, 0666);
   if ( f <  0 ) {
      perror("Failed to open trace_file\n" );
      exit(EXIT_FAILURE);
   }
   trace_fd = f;
}

/* helper function useful to dump raw packets for analysis */
void
hexdump( const char *buffer, int len) {
   for( int i=0; i<len; i++) {
      DEBUG("0x%x ", buffer[i]);
      if (i%8 ==0)
         TRACE("\n");
   }
   TRACE("\n");
}

/* circular queue stuff */
struct qentry {
   char buffer[MQTT_MSG_MAX_LEN];
   int message_len;
};

struct message_queue {
   struct qentry entry[ QUEUE_LEN_MAX ];
   int head;
   int tail;
};

void
queue_enq(struct message_queue *queue, const char *message, int size) {
   if ((queue->tail + 1)%QUEUE_LEN_MAX == queue->head) {
      // FULL CASE
      perror("queue full");
      exit(EXIT_FAILURE);
   }
   if (queue->head == queue->tail && queue->head == -1 ) {
      queue->head = queue->tail = 0;
   } else {
      queue->tail = (queue->tail + 1)  % QUEUE_LEN_MAX;
   }
   memcpy(queue->entry[ queue->tail ].buffer, message, size);
   queue->entry[ queue->tail ].message_len = size;
}

int
queue_deq(struct message_queue *queue) {
   if (queue->head == queue->tail && queue->head == -1 ) {
      // can't delete entry in an empty queue
      return -1;
   }
   if (queue->head == queue->tail) {
      // all entries deleted.
      int last_entry = queue->head;
      queue->head = queue->tail = -1;
      return last_entry;
   }
   int last_entry = queue->head;
   queue->head = (queue->head+1)%QUEUE_LEN_MAX;
   return last_entry;
}

struct message_queue receive_queue = {0};
struct message_queue send_queue = {0};

void
queue_init() {
   receive_queue.head = receive_queue.tail = -1;
   send_queue.head = send_queue.tail = -1;
}
/* circular queue stuff ends */

pthread_mutex_t receive_queue_lock;
pthread_mutex_t send_queue_lock;

#define SEND(message, length) do {              \
   pthread_mutex_lock(&send_queue_lock);        \
   queue_enq(&send_queue, message, length);     \
   pthread_mutex_unlock(&send_queue_lock);      \
   } while(0);                                  \

#define RECEIVE(message, length) do {              \
   pthread_mutex_lock(&receive_queue_lock);        \
   queue_enq(&receive_queue, message, length);     \
   pthread_mutex_unlock(&receive_queue_lock);      \
   } while(0);                                     \

#define SEND_DEQ( index ) do {                  \
   pthread_mutex_lock(&send_queue_lock);        \
   index = queue_deq(&send_queue);              \
   pthread_mutex_unlock(&send_queue_lock);      \
   } while(0);                                  \


#define BUILD_GET_QUEUE_SIZE( queue_type )               \
int get_queue_size_##queue_type() {                      \
   struct message_queue *queue = &queue_type##_queue;    \
   pthread_mutex_lock(&queue_type##_queue_lock);         \
   if (queue->head == -1) {                              \
      pthread_mutex_unlock(&queue_type##_queue_lock);    \
      return 0;                                          \
   }                                                     \
   if (queue->head < queue->tail) {                      \
      pthread_mutex_unlock(&queue_type##_queue_lock);    \
      return (queue->tail - queue->head + 1);            \
   }                                                     \
   pthread_mutex_unlock(&queue_type##_queue_lock);       \
   return (queue->head - queue->tail + 1);               \
}                                                        \

BUILD_GET_QUEUE_SIZE( send ); // get_queue_size_send()
BUILD_GET_QUEUE_SIZE( receive ); // get_queue_size_receive()

char conn_resp[] = { 4, 0x20, 0x02, 0x00, 0x00 };
char subscribe_resp[] = { 5, 0x90, 0x03, 0x00, 0x00 /* len */,  0x00 };
char ping_resp[] = { 2, 0xd0, 0x00 };
char unknown_resp[] = { 1, 0x00 };

char *response_list[]= {
   unknown_resp, conn_resp, ping_resp, subscribe_resp,
};

struct types {
   char type;
   char *name;
};

enum {
   RESERVED,
   CONNECT,
   CONNACK,
   PUBLISH,
   PUBACK,
   PUBREC,
   PUBREL,
   PUBCOMP,
   SUBSCRIBE,
   SUBACK,
   UNSUBSCRIBE,
   UNSUBACK,
   PINGREQ,
   PINGRESP,
   DISCONNECT,
   FORBIDDEN,
};

struct types message_type[][2] = {
   SET_TYPE(RESERVED),
   SET_TYPE(CONNECT),
   SET_TYPE(CONNACK),
   SET_TYPE(PUBLISH),
   SET_TYPE(PUBACK),
   SET_TYPE(PUBREC),
   SET_TYPE(PUBREL),
   SET_TYPE(PUBCOMP),
   SET_TYPE(SUBSCRIBE),
   SET_TYPE(SUBACK),
   SET_TYPE(UNSUBSCRIBE),
   SET_TYPE(UNSUBACK),
   SET_TYPE(PINGREQ),
   SET_TYPE(PINGRESP),
   SET_TYPE(DISCONNECT),
   SET_TYPE(FORBIDDEN)
};


struct client_info {
   pthread_t thread;
   pthread_t send_thread;
   int client_id;
   int socket_fd;
   volatile bool thread_cancel;
};

struct __attribute__((__packed__)) mqtt_header {
   char flags : 4; // little endian so adding flag first
   char message_type : 4;
   char length;
};

struct __attribute__((__packed__)) connect_ack {
   char flags;
   char ret_code;
};

struct __attribute__((__packed__)) connect_req {
   char protocol_name_len[2];
   char proto_name[4]; // keeping it 4 for MQTT
   char version; 
   char flags;
   char keep_alive[2];
   char client_id_len[2];
   char client_info;
};

struct __attribute__((__packed__)) sub_req {
   char identifier[2];
   char topic_len[2];
   char topic_and_qos;
};

struct __attribute__((__packed__)) sub_ack {
   char identifier[2];
   char qos;
};

struct __attribute__((__packed__)) pub_req {
   char topic_len[2];
   char topic_and_message;
};

struct __attribute__((__packed__)) mqtt_packet {
   struct mqtt_header header;
   union {
      struct connect_req conn;
      struct connect_ack connack;
      struct sub_req sub;
      struct sub_ack suback;
      struct pub_req pub;
      char identifier[2]; // not present in CONN/CONNACK/PING/DISCONN/PUB (if qos=0)
      char payload;
   } body;
};

typedef union __attribute__((__packed__)) {
   struct mqtt_packet *pkt;
   char *buffer;
} mqtt_pkt_ptr;

// Generic publish message.
struct json_pub_message {
   int timestamp;
   int key;
   int value;
   bool is_v_set;
};

int
serialize_pub_message(struct json_pub_message message, char *out_message) {
   char value[MQTT_MSG_MAX_LEN] = {0};
   int val_len = 0;
   if (message.is_v_set) {
      val_len = snprintf(value, MQTT_MSG_MAX_LEN, ",\"v\":\"%d\"", message.value);
   }
   return snprintf(out_message, MQTT_MSG_MAX_LEN, "{\"t\":\"%d\",\"k\":%d%.*s}",
                   message.timestamp, message.key, val_len, value);
}


void
dump_mqtt_packet(char *buffer, int size) {
   mqtt_pkt_ptr p;
   p.buffer = buffer;
   INIT_TRACE_TO_BUFFER;
   
   char tabs[][16] = {
      "   ", "      ", "         "
      };
   // Dump header
   TRACE_TO_BUFFER("MQTT Header\n");
   int msg_type = p.pkt->header.message_type & 0xf;
   TRACE_TO_BUFFER("%sMessage Type: %s\n", tabs[0], MSG_TYPE(msg_type));
   TRACE_TO_BUFFER("%sFlags: %d\n", tabs[0], p.pkt->header.flags);
   TRACE_TO_BUFFER("%sMessage Length: %d\n", tabs[0], p.pkt->header.length);
   int len = 0;
   char *msg = NULL;

   switch( msg_type ) {
      case CONNECT:
         TRACE_TO_BUFFER("%sProto Name Len: %d\n", tabs[1],
                        BYTE2_INT(p.pkt->body.conn.protocol_name_len));
         TRACE_TO_BUFFER("%sProto Name: %.4s\n", tabs[1], p.pkt->body.conn.proto_name);
         TRACE_TO_BUFFER("%sVersion: 0x%x\n", tabs[1], p.pkt->body.conn.version);
         TRACE_TO_BUFFER("%sFlags: 0x%x\n", tabs[1], p.pkt->body.conn.flags);
         TRACE_TO_BUFFER("%sKeepAlive: %ds\n", tabs[1], BYTE2_INT(p.pkt->body.conn.keep_alive));
         len = BYTE2_INT(p.pkt->body.conn.client_id_len); 
         TRACE_TO_BUFFER("%sClientIdLen: %d\n", tabs[1], len);
         TRACE_TO_BUFFER("%sClientId: %.*s\n", tabs[1], len, &p.pkt->body.conn.client_info);
         TRACE_TO_BUFFER("%sQoS: %x\n", tabs[1], *(&p.pkt->body.conn.client_info + len));
         break;
      case CONNACK:
         TRACE_TO_BUFFER("%sFlags: 0x%x\n", tabs[1], p.pkt->body.connack.flags);
         TRACE_TO_BUFFER("%sRetCode: 0x%x\n", tabs[1], p.pkt->body.connack.ret_code);
         break;
      case PINGREQ:
      case PINGRESP:
         /* No message body */
         break;
      case SUBSCRIBE:
         TRACE_TO_BUFFER("%sIdentifier: %d\n", tabs[1], BYTE2_INT(p.pkt->body.sub.identifier));
         len = BYTE2_INT(p.pkt->body.sub.topic_len); 
         TRACE_TO_BUFFER("%sTopicLen: %d\n", tabs[1], len);
         TRACE_TO_BUFFER("%sTopic: %.*s\n", tabs[1], len, &p.pkt->body.sub.topic_and_qos);
         TRACE_TO_BUFFER("%sQoS: %x\n", tabs[1], *(&p.pkt->body.sub.topic_and_qos + len));
         break;
      case SUBACK:
         TRACE_TO_BUFFER("%sIdentifier: %d\n", tabs[1], BYTE2_INT(p.pkt->body.suback.identifier));
         TRACE_TO_BUFFER("%sQoS: %x\n", tabs[1], p.pkt->body.suback.qos);
         break;
      case PUBLISH:
         len = BYTE2_INT(p.pkt->body.pub.topic_len); 
         TRACE_TO_BUFFER("%sTopicLen: %d\n", tabs[1], len);
         TRACE_TO_BUFFER("%sTopic: %.*s\n", tabs[1], len, &p.pkt->body.pub.topic_and_message);
         msg = (&p.pkt->body.pub.topic_and_message + len);
         len = size - (msg - buffer);
         TRACE_TO_BUFFER("%sMessage: %.*s (size: %d)\n", tabs[1], len, msg, len);
         break;
      default:
         TRACE_TO_BUFFER("%sUNKNOWN PACKET TYPE\n", tabs[1]);
   }
   TRACE_TO_BUFFER_FLUSH;

}

char*
get_response( int request_and_flag, char * buffer ) {
   if (IS_REQUEST(request_and_flag, CONNECT)) { // CONNECT
      return response_list[1];
   }
   if (IS_REQUEST(request_and_flag, PINGREQ)) { // PING
      return response_list[2];
   }
   if (IS_REQUEST(request_and_flag, SUBSCRIBE)) { // SUBSCRIBE
      char * resp = response_list[3];
      resp[4] = buffer[3]; // copy the message id from request and send ack
      return resp;
   }
   TRACE("Sent no response, MessageType: %s\n",
         MSG_TYPE((request_and_flag >> 4) & 0xf));
   return response_list[0];
}

int
build_publish_request(struct json_pub_message json_message,
                          char buffer[], int buffer_size) {
   mqtt_pkt_ptr pp;
   pp.buffer = &buffer[0];
   pp.pkt->header.flags = 0x0;
   pp.pkt->header.message_type = PUBLISH;
   const char *topic = "/device/id/" DEVICE_ID;
   // setting payload i.e. topic and message
   int topic_len = (int)strlen(topic);
   INT_TO_BYTE2( topic_len, pp.pkt->body.pub.topic_len );
   memcpy(&pp.pkt->body.pub.topic_and_message, topic, topic_len); 
   char* message_begin = (&(pp.pkt->body.pub.topic_and_message)+ topic_len);
   int message_size = serialize_pub_message(json_message, message_begin);
   pp.pkt->header.length = topic_len + message_size + 2;
   int pkt_size = sizeof(pp.pkt->header) + pp.pkt->header.length;
   return pkt_size;
}

void*
process_client_request(void* cinfo ) {
   struct client_info* client_info_p = (struct client_info*)cinfo;
   char buffer[ BUFFER_SIZE ] = {0};
	int bytes_read;
   int client_id = client_info_p->client_id;
   char client_trace[30] = {0};
   int epoll_fd = epoll_create1(0);
   
   struct epoll_event event;
   event.data.fd = client_info_p->socket_fd;
   event.events = EPOLLIN; 
   epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_info_p->socket_fd, &event);

   int received_tt = 30; // let default timeout be 30s, will be adjusted later.
   int timeout = received_tt * 1000; // 10 seconds
   sprintf(client_trace, "ClientID: %d\n", client_id);

   while(!client_info_p->thread_cancel) {
      int e_ret = 0;
      e_ret = epoll_wait(epoll_fd, &event, 1, timeout);
      if (e_ret == -1) {
         DEBUG("\nclient disconnected\n");
         return NULL;
      }
      bytes_read = read(client_info_p->socket_fd, buffer, BUFFER_SIZE-1);
      if (e_ret == 0 && bytes_read == -1) {
         perror("\nclient disconnected due to timeout" );
         exit(EXIT_FAILURE);
      }
      if (!bytes_read) {
         DEBUG("\nclient exited\n");
         return NULL;
      }

      TRACE("ID: %d Received Packet\n", client_id );
      dump_mqtt_packet(buffer, bytes_read);

      if (IS_REQUEST(buffer[0], CONNECT)) {
         // if this is a CONNECT request, update keep alive timer
         mqtt_pkt_ptr parsed = { .buffer=&buffer[0] };
         received_tt = BYTE2_INT(parsed.pkt->body.conn.keep_alive);
         TRACE( "\nSet timeout to %ds\n", received_tt);
      }
      
      /* send response if needed */
      char * response = get_response(buffer[0], buffer);
      if (response[1]) { /* if the request needs a response */
         SEND(&response[1], (int)response[0]);
      }
   };
}

void*
process_client_send(void* cinfo) {
   struct client_info* client_info_p = (struct client_info*)cinfo;
   char buffer[ BUFFER_SIZE ] = {0};
	int bytes_read;
   int client_id = client_info_p->client_id;
   char client_trace[30] = {0};
   int limit = 12;

   while(!client_info_p->thread_cancel) {
      if (get_queue_size_send()) {
         int index = -1;
         SEND_DEQ( index );
         send(client_info_p->socket_fd, &send_queue.entry[ index ].buffer,
              send_queue.entry[ index ].message_len, 0);
         TRACE("ID: %d Sent Packet\n", client_id );
         dump_mqtt_packet(&(send_queue.entry[ index ].buffer[0]),
                          send_queue.entry[ index ].message_len);
      }
   }
}

struct json_pub_message
str_to_message(char *line, int t, char **cmd_type) {

   struct json_pub_message message = {
      .timestamp = t,
      .key = 0,
      .value = 0,
      .is_v_set = false
   };
   char * tokens[3] = {NULL, NULL, NULL};
   *cmd_type = line; // set cmd_type to start of the string (initialize)
   int i = 0;
   tokens[i++] = strtok( line, " \n" );
   while (i<3) {
      tokens[i++]= strtok(NULL, " \n");
   }
   if (tokens[0]) {
      *cmd_type = tokens[0];
   }
   if (tokens[1]) {
      message.key = atoi(tokens[1]);
   }
   if (tokens[2]) {
      message.value = atoi(tokens[2]);
      message.is_v_set = true;
   }
   return message;
}

void
start_prompt() {
   char *line = NULL;
   size_t line_size = 0;
   int input_size = 0; // no of bytes read from stdin
   do {
      DEBUG(">>> ");

      input_size = getline(&line, &line_size, stdin);
      if (input_size < 0) {
         DEBUG("\n>>> exit\n");
         break; //error
      }
      line[input_size-1] = '\0';

      if(input_size > 1) { // more than just a new line
         char *cmd = NULL; // cmd is a pointer in line buffer
         struct json_pub_message message = str_to_message( line, CONST_TIMESTAMP,
                                                           &cmd);
         DEBUG( "Cmd:   '%s' %d\n", cmd, strlen(cmd));
         DEBUG( "Key:   '%d' %d\n", message.key );
         DEBUG( "Value: '%d' %d\n", message.value );

         // cmd is not being used, it can be used to send different types of
         // response
         char serialized_msg_buffer[MQTT_MSG_MAX_LEN];
         serialize_pub_message( message, serialized_msg_buffer);
         DEBUG("Serialized: %s\n", serialized_msg_buffer);

         char send_buffer[BUFFER_SIZE] = {0};
         int respsize = build_publish_request( message, send_buffer, BUFFER_SIZE );
         SEND(&send_buffer[0], respsize);
         DEBUG("\n");
      }
   } while(true);
   free(line);
}

int main(int argc, const char* argv[]) {
	int server_fd, new_socket;
	struct sockaddr_in address;
	int opt = 1;
	socklen_t addrlen = sizeof(address);
	char buffer[BUFFER_SIZE] = {0};

   /* write PID to a file for utils */
   char *filename = "portfile";
   int fd = open(filename, O_WRONLY | O_TRUNC | O_CREAT, 0666 );
   sprintf(buffer, "%d", getpid());
   write(fd, buffer, strlen(buffer));
   close(fd);

   init_tf(); // intialize trace file
   TRACE("PID: %d FD: %d\n", getpid(), fd);

   /* create a TCP socket */
	if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		perror( "Socket creation failed");
		exit(EXIT_FAILURE);
	}

   /* set socket options */
	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT,
						 &opt, sizeof(opt))) {
			perror( "setsockopt error" );
		   exit(EXIT_FAILURE);
	}

   /* set addr:port info for the server to listen to */
   address.sin_family = AF_INET;
   address.sin_addr.s_addr = INADDR_ANY;
   address.sin_port = htons(PORT);

   if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
      perror( "bind failed" );
      exit(EXIT_FAILURE);
   }

   /* start listening to clients */
   if (listen(server_fd, 2) < 0) {
      perror( "listen failed" );
      exit(EXIT_FAILURE);
   }
   TRACE("Listening on %d\n", PORT);
   /* supports single client at the moment, can be extended */
   int requests = 1;
   struct client_info clients[requests];
   int cid=0;

   pthread_mutex_init(&receive_queue_lock, NULL);
   pthread_mutex_init(&send_queue_lock, NULL);
   queue_init();

   /* accept a new client */
   if ((new_socket = accept(server_fd, (struct sockaddr*)&address, &addrlen)) < 0 ) {
      perror( "accept failed" );
      exit(EXIT_FAILURE);
   }
   /* set socket to be non-blocking for read calls */
   if (fcntl(new_socket, F_SETFL, fcntl(new_socket, F_GETFL, 0) | O_NONBLOCK ) < 0) {
      perror( "fcntl failure" );
      exit(EXIT_FAILURE);
   }
   
   /* setup new client info */
   clients[ cid ].client_id = cid;
   clients[ cid ].socket_fd = new_socket;
   clients[ cid ].thread_cancel = false;
   int res = pthread_create( &clients[ cid ].thread, NULL,
                             &process_client_request, (void*)&clients[cid]);
   if (res) {
      perror("error in creating read thread");
      exit(EXIT_FAILURE);
   }
   res = pthread_create( &clients[ cid ].send_thread, NULL,
                         &process_client_send, (void*)&clients[cid]);
   if (res) {
      perror("error in creating send thread");
      exit(EXIT_FAILURE);
   }

   /* start an interactive prompt for the user to send requests */
   start_prompt();

   clients[ cid ].thread_cancel = true;
   DEBUG("Waiting for threads to exit...\n");
   pthread_join(clients[cid].thread, NULL);
   pthread_join(clients[cid].send_thread, NULL);
   close(new_socket);
   close(server_fd);
   return 0;
}
