#include <atomic>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <atomic>

int update = 2;

void nestedtIncrementByTwo()
{
    __transaction_atomic
    {
    __transaction_atomic{
        update = update + 1;//second increment by inner transaction
        __transaction_cancel;
        throw std::logic_error("abort transaction");
    }
    update=update+1;//first increment by outer transaction   

    }
}

// void nestedDecrementByTwo()
// {
//     __transaction_atomic
//     {
//         update=update-1;//first decrement by outer transaction
//         __transaction_atomic
//         {

//             update = update - 1;//2nd decrement by inner transaction
           
//         }
//     }
// }

int main()
{
     std::thread t1(nestedtIncrementByTwo);
     sleep(3);
     t1.join();

    std::cout << " the value of update :: " << update << " \n ";
    return 0;
}

