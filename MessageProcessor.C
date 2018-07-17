#include <iostream>
#include <thread>
#include <queue>
#include <deque>
#include <vector>
#include <chrono>
#include <atomic>
#include <mutex>
#include <algorithm>
#include <string>
#include <chrono>
using namespace std;

enum class MessageType
{
	CANCEL,
	NEW,
	MODIFY
};

class Message
{

	public:
		unsigned int m_msgId{ 0 };
		MessageType m_msgType{ MessageType::NEW };
		string m_msg{ "" };
		bool m_validMessage{ false };
		Message() = default;
		Message( const MessageType& msgType, int msgId, const string& msg ): m_msgId( msgId ), m_msgType( msgType ), m_msg( msg )
		{
			m_validMessage = true;
		}
		bool send()
		{
			//cout << cout << "Thread_Id :" <<  std::this_thread::get_id() << ":- Sending message with id:" << m_msgId << " msgType :" << static_cast<int>( m_msgType ) << " Msg:" << m_msg << endl;
			return true;
		}
		MessageType getMessageType() const
		{
			return m_msgType;
		}
		unsigned int getMessageId() const
		{
			return m_msgId;
		}
		bool isValid() const
		{
			return m_validMessage;
		}
				
};

class MessageQueue 
{
	mutable std::mutex m_mutex;
	std::queue<Message> m_messageQueue;
	bool empty()
	{
		return m_messageQueue.empty();
	}
	public:
		MessageQueue& operator=( MessageQueue& ) = delete;
		MessageQueue( MessageQueue& ) = delete;
		MessageQueue() = default;
		~MessageQueue() = default;
		
		void push( const Message& msg );
		bool popValue( Message& msg );
};
		
void MessageQueue::push( const Message& msg )
{
	std::lock_guard<std::mutex> lk( m_mutex );
	m_messageQueue.push( msg );
}

bool MessageQueue::popValue( Message& msg )
{
	std::lock_guard<std::mutex> lk( m_mutex );
	if( empty() )
	{
		msg = Message();
		return false;
	}
	msg = m_messageQueue.front();
	m_messageQueue.pop();
	return true;
}

class MessageDoubleEndedQueue 
{
	mutable std::mutex m_mutex;
	std::deque<Message> m_deQueue;
	bool empty() 
	{
		return m_deQueue.empty();
	}
	public:
		MessageDoubleEndedQueue& operator=( MessageQueue& ) = delete;
		MessageDoubleEndedQueue( MessageQueue& ) = delete;
		MessageDoubleEndedQueue() = default;
		~MessageDoubleEndedQueue() = default;
		
		void insertMessageByMsgType( const Message& msg );
		bool popValue( Message& msg );
};
		
void MessageDoubleEndedQueue::insertMessageByMsgType( const Message& msg )
{
	
	if( msg.getMessageType() == MessageType::CANCEL )
	{
		std::lock_guard<std::mutex> lk( m_mutex );
		m_deQueue.push_front( msg );
	}
	else
	{
		std::lock_guard<std::mutex> lk( m_mutex );
		m_deQueue.push_back( msg );
	}
}

bool MessageDoubleEndedQueue::popValue( Message& msg )
{
	std::lock_guard<std::mutex> lk( m_mutex );
	if( empty() )
	{
		msg = Message();
		return false;
	}
	msg = m_deQueue.front();
	m_deQueue.pop_front();
	return true;
}

		
class MessageProcessor
{
		std::vector<std::thread> m_threads;
		MessageQueue m_msgQueue;
		MessageDoubleEndedQueue	m_OrderedMsgQueue;
		std::mutex m_mutex;
		// for debugging printing
		std::mutex m_prntMutex;
		atomic<bool> m_done{false};
		
		unsigned int m_noOfMessageSent{0};
		unsigned int m_noOfMessagePerSecond{0};
		unsigned int m_messageIntervalInMilliseconds{0};
		unsigned int m_noOfMessageSentOneSecondAgo{0};
		
		
		using tPoint = std::chrono::high_resolution_clock::time_point;
		using time = std::chrono::high_resolution_clock;
		tPoint m_firstMsgSentTime{ time::now() };
		tPoint m_lastMsgSentTime{ time::now() }; 
		tPoint m_timeWindow{ time::now() };
		std::once_flag m_onceFlagRecordFirstMessage;
		
		void recordTimeOfFirstMessage();
		void orderMessage_thread();
		void sendMessage_thread();
		void checkMessagesSentPerSecond_thread();
		void checkMessageInterval();
				
	public:
		
		MessageProcessor( unsigned int msgPerSecond );
		MessageProcessor() = default;
		~MessageProcessor();
		MessageProcessor& operator=( const MessageProcessor& ) = delete;
		MessageProcessor( const MessageProcessor& ) = delete;
		
		void stopThreads();
		bool addMessage( const Message&& msg );
		void displayStatistics();
		bool run();
};


	
MessageProcessor::MessageProcessor( unsigned int msgPerSecond ): MessageProcessor()
{
	// Minimum sleep time 1 sec / 100 messages = 10 milliseconds
	m_noOfMessagePerSecond = msgPerSecond;
	m_messageIntervalInMilliseconds = 1000 / m_noOfMessagePerSecond;
	cout << "No of message to send per second :" << m_noOfMessagePerSecond << endl;
	cout << "Minimum sleep in milliSeconds :" << m_messageIntervalInMilliseconds << endl;
	cout << "-----------------------------" << endl;
}

void MessageProcessor::recordTimeOfFirstMessage()
{
	m_firstMsgSentTime = time::now();
	m_timeWindow = time::now();
}

bool MessageProcessor::run()
{
	cout << "Start Processing Messages..." << endl;
	int thread_count = std::thread::hardware_concurrency();
	if( thread_count <= 0 )
	{
		thread_count = 8;
	}
	//unsigned const thread_count = 20;
	cout << "Recommended threads can truly run parallel :" << thread_count << endl;
	m_threads.push_back( std::thread( &MessageProcessor::checkMessagesSentPerSecond_thread, this ) );
	
	for( int i=0; i<thread_count/2; ++i )
	{
		m_threads.push_back( std::thread( &MessageProcessor::orderMessage_thread, this ) );
	}
	for( int i=0; i<thread_count/2; ++i )
	{
		m_threads.push_back( std::thread( &MessageProcessor::sendMessage_thread, this ) );
	}
}

MessageProcessor::~MessageProcessor()
{
	for_each(m_threads.begin(), m_threads.end(), std::mem_fn( &std::thread::join ) );
}

void MessageProcessor::orderMessage_thread()
{
	std::unique_lock<std::mutex> prntUlk( m_prntMutex );
	cout << "Thread_Id :" <<  std::this_thread::get_id() << " :- Starting orderMessage_thread - " << endl;
	prntUlk.unlock();
	
	while( !m_done )
	{
		Message msg;
		if( !m_msgQueue.popValue( msg ) )
		{
			std::this_thread::yield();				
		}
		if( msg.isValid() )
		{
			m_OrderedMsgQueue.insertMessageByMsgType( msg );
		}
		std::this_thread::sleep_for( std::chrono::milliseconds( 1 ) );
	}	
}

void MessageProcessor::checkMessagesSentPerSecond_thread()
{
	std::unique_lock<std::mutex> prntUlk( m_prntMutex );
	cout << "Thread_Id :" <<  std::this_thread::get_id() << " :- Starting checkMessagesSentPerSecond_thread - " << endl;
	prntUlk.unlock();
	while( !m_done )
	{
		{
			std::lock_guard<std::mutex> lk( m_mutex );
			unsigned int timeGapInMilliSeconds = std::chrono::duration_cast<std::chrono::milliseconds>( time::now() - m_timeWindow ).count();
			if( timeGapInMilliSeconds >= 1000 )
			{
				m_timeWindow = time::now();
				unsigned int msgSentLastOneSecond = m_noOfMessageSent - m_noOfMessageSentOneSecondAgo;
				m_noOfMessageSentOneSecondAgo = m_noOfMessageSent;
				prntUlk.lock();
				cout << "Thread_Id :" << std::this_thread::get_id() << " :- Message sent in last " << 
					timeGapInMilliSeconds/1000 << " second ( milliseconds :" << timeGapInMilliSeconds << " ):" << msgSentLastOneSecond << endl;
				prntUlk.unlock();
			
				if( msgSentLastOneSecond > m_noOfMessagePerSecond )
				{
					prntUlk.lock();
					cout << "Thread_Id :" << std::this_thread::get_id() << "  :- Message sent in last " << 
							timeGapInMilliSeconds/1000 << " second ( milliseconds :" << 
							timeGapInMilliSeconds << " ):" << msgSentLastOneSecond <<
							" Sleeping for :" << m_messageIntervalInMilliseconds << " milliseconds..." << endl;
					prntUlk.unlock();
				
					std::this_thread::sleep_for( std::chrono::milliseconds( m_messageIntervalInMilliseconds ) );
				}	
			}	
		}
		std::this_thread::sleep_for( std::chrono::milliseconds( 1 ) );
	}
}


void MessageProcessor::checkMessageInterval() 
{
	// Mutex is locked by the caller
	unsigned int timeGapInMilliSeconds = std::chrono::duration_cast<std::chrono::milliseconds>( time::now() - m_lastMsgSentTime ).count();
	if( timeGapInMilliSeconds < m_messageIntervalInMilliseconds )
	{
		unsigned int sleepTime = m_messageIntervalInMilliseconds - timeGapInMilliSeconds;
		//std::unique_lock<std::mutex> prntUlk( m_prntMutex );
		//cout << std::this_thread::get_id() << " :- MessageProcessor::checkMessageInterval Sleeping for :" << sleepTime << " milliseconds" << endl;
		//prntUlk.unlock();
		std::this_thread::sleep_for( std::chrono::milliseconds( sleepTime ) );
	}
}

void MessageProcessor::sendMessage_thread()
{
	std::unique_lock<std::mutex> prntUlk( m_prntMutex );
	cout << "Thread_Id :" << std::this_thread::get_id() << " :- Starting sendMessage_thread - " << endl;
	prntUlk.unlock();
	while( !m_done )
	{
		Message msg;
		if( !m_OrderedMsgQueue.popValue( msg ) )
		{
			std::this_thread::yield();
		}
		if( msg.isValid() )
		{
			std::call_once( m_onceFlagRecordFirstMessage,  &MessageProcessor::recordTimeOfFirstMessage, this );
			std::lock_guard<std::mutex> lk( m_mutex );
			checkMessageInterval();
			if( msg.send() )
			{
				m_lastMsgSentTime = time::now();
				++m_noOfMessageSent;
			}	
		}
		std::this_thread::sleep_for( std::chrono::milliseconds( 1 ) );
	}
}	

bool MessageProcessor::addMessage( const Message&& msg )
{
	m_msgQueue.push( std::move( msg ) );	
}
void MessageProcessor::stopThreads()
{
	m_done.store( true );
}
void MessageProcessor::displayStatistics()
{
	std::lock_guard<std::mutex> lktimeMutex( m_mutex );
	unsigned int timeGapInMilliSeconds = std::chrono::duration_cast<std::chrono::milliseconds>( m_lastMsgSentTime - m_firstMsgSentTime ).count();
	std::unique_lock<std::mutex> prntUlk( m_prntMutex );
	cout << "-----------------------------" << endl;
	cout << "Total messages sent out :" << m_noOfMessageSent << " in :" << timeGapInMilliSeconds/1000 << 
			" seconds ( milliSeconds:" << timeGapInMilliSeconds << " )" << endl;
	cout << "-----------------------------" << endl;
	prntUlk.unlock();
}

void usage()
{
	cout << "Usage :./MessageProcessor -msgPerSec <msgPerSec>  -noOfMsgToSend <noOfMsgToSend> " << endl;
}

int main( int argc, char** argv )
{
	int numberOfMsgPerSecond = 0;
	int numberOfMessages = 0;
	if( argc < 5)
	{
		usage();
		return -1;
	}
	cout << "-----------------------------" << endl;
	int i=0;
	while( i < argc )
	{
		if( string( argv[i] ) == string( "-msgPerSec" ) )
		{
			numberOfMsgPerSecond = std::stoi( string( argv[i +1] ) );
			cout << "-msgPerSec captured:" << argv[i +1] << endl;
		}
		else if( string( argv[i] ) == string( "-noOfMsgToSend" ) )
		{
			numberOfMessages = std::stoi( string( argv[i +1] ) );
			cout << "-noOfMsgToSend captured:" << argv[i +1] << endl;
		}
		++i;
	}
	if( numberOfMsgPerSecond == 0 || numberOfMessages == 0 )
	{
		cout << "Please check value passed - arguments are case sensitive..." << endl;
		usage();
		return -1;
	}
	cout << "-----------------------------" << endl;
	cout << "Total Messages to be sent out :" << numberOfMessages << endl;
	MessageProcessor messageProcessor( numberOfMsgPerSecond );
	messageProcessor.run();
	for( int i =1; i< numberOfMessages +1; ++i )
	{
		
		if( i%2 == 0 )
			messageProcessor.addMessage( Message(MessageType::CANCEL, i, string( "Trade info:" + std::to_string( (long long ) i ) ) ) ) ;
		else
			messageProcessor.addMessage( Message(MessageType::NEW, i, string( "Trade info:" + std::to_string( ( long long ) i ) ) ) ) ;	
							
	}
	// This need to be adjusted...
	std::this_thread::sleep_for( std::chrono::seconds( numberOfMessages/numberOfMsgPerSecond * 2 ));
	messageProcessor.stopThreads();
	messageProcessor.displayStatistics();
	return 0;
}
