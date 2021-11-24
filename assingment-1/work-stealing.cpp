#include <atomic>
#include <iostream>
#include <random>
#include <thread>
#include <unistd.h>
std::atomic<bool> terminate;
////////////////////////////////////////////////////////////////////////////////
#include "deque.cpp"
const int N = 4; // number of processors
DQueue job_queue[N];
void processor(int n)
{
  while (!::terminate)
    {
        int duration;
        int index=1;
        __transaction_atomic{
       duration=job_queue[n].PopLeft();
        }
        if (duration != -1)
        {
     __transaction_relaxed{
         std::cout << "Processor " << n << ": executing job (duration: " << duration << "s)."<< std::endl;
                            }              
        }
        while(duration == -1 || !::terminate){
            index=(index+n)%N;
            int steal_duration;
            __transaction_atomic{
                steal_duration=job_queue[index].PopRight();
                if(steal_duration==0){
                        job_queue[index].PushRight(steal_duration); 
                }
            }
                if(steal_duration==-1){
                        index=index+1;//goto next queue, since current queue is empty
                }
                if(steal_duration>0){
                                __transaction_relaxed{
                  std::cout << "Processor " << n << ": stealing job (duration: " << steal_duration << "s) from processor " << index << std::endl;
        }                    
                }

        
        }

        
        
    }
}
////////////////////////////////////////////////////////////////////////////////
void user()
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> processor(0, N-1);
  std::uniform_int_distribution<int> duration(0, 2*N);
  unsigned int time = 0;
  // generates a new job about once per second
  while (!::terminate)
    {
      int p = processor(gen);
      int d = duration(gen);
      __transaction_relaxed
        {
          std::cout << time << ": scheduling a job on processor " << p << " (duration: " << d << " s)." << std::endl;
          job_queue[p].PushRight(d);
        }
      sleep(1);
      ++time;
    }
}
////////////////////////////////////////////////////////////////////////////////
int main()
{
  std::thread user_thread(user);
  std::thread processors[N];
  for (int i=0; i<N; ++i)
    {
      processors[i] = std::thread(processor, i);
    }
  sleep(30);
  ::terminate = true;
  user_thread.join();
  for (int i=0; i<N; ++i)
    {
      processors[i].join();
    }
  return 0;
}
