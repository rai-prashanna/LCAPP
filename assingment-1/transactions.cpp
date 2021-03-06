#include <atomic>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <atomic>

std::atomic<bool> terminate;
int x, y, diff;

void check()
{
  while (!terminate)
      {
        __transaction_atomic{
        if (x != y)
          ++diff;
        }
      }
}

void toggle()
{
  while (!terminate)
      {
        __transaction_atomic{
        x = 1-x;
        y = 1-y;
       }
      }
}

int main()
{
  std::thread t1(check);
  std::thread t2(toggle);

  sleep(1);

  terminate = true;

  t1.join();
  t2.join();

  std::cout << "diff: " << diff << std::endl;

  return 0;
}
