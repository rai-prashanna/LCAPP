#include <atomic>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <atomic>

int update = 2;

void incrementByTwo()
{
    __transaction_atomic
    {
        update = update + 2;
    }
}

void unitdecrement()
{
    try
    {
        __transaction_atomic
        {
            update = update - 1;
            throw std::logic_error("abort transaction");

        }
   }
    catch (std::exception &e)
    {
        std::cout << "After exception the value of update :: " << update<<" \n";
    }
}

int main()
{
        std::thread t1(incrementByTwo);
        std::thread t2(unitdecrement);
        sleep(3);
        t1.join();

        std::cout << "after unitincrement thread , the value of update :: " << update<<" \n ";
        t2.join();

        std::cout << "after unitdecrement thread , the value of update :: " << update<<" \n ";
    return 0;
}
