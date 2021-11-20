#include <iostream>
#include <thread>

void f1()
{
  __transaction_atomic
    {
      std::cout << "Hello " << std::flush;
      std::cout << "world!" << std::flush;
    }
}

void f2()
{
  std::cout << "brave new " << std::flush;
}

int main()
{
  std::thread t1(f1);
  std::thread t2(f2);

  t1.join();
  t2.join();

  return 0;
}
