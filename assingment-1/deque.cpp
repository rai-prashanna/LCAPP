#include <stdio.h>      /* printf, scanf, NULL */
#include <stdlib.h>  
#include <cstddef>
#include <iostream>
#include <thread>
#include <atomic>

using namespace std;

class DQueue
{
  const int leftSentinelValue = -2147483648;
  const int rightSentinelValue = 2147483647;
  
  struct QNode
  {
    QNode *left, *right;
    int val;
  };

  QNode *leftSentinel, *rightSentinel;

public:

  DQueue()
  {
    leftSentinel= (QNode*) malloc(sizeof(QNode));
    leftSentinel->val=leftSentinelValue;
    leftSentinel->left=NULL;
    // cout<< leftSentinel;
    // cout<< "\n";
    // cout<< leftSentinelValue;
    //  cout<< "\n";
    //create rightSentinelNode
    rightSentinel= (QNode*) malloc(sizeof(QNode));
    rightSentinel->val=rightSentinelValue;
    rightSentinel->right=NULL;
    // cout<< rightSentinel;
    // cout<< "\n";
    // cout<< rightSentinelValue;
    // cout<< "\n";

    //connect leftSentinel Node with rightSentinel
    leftSentinel->right=rightSentinel;
    //connect rightSentinel Node with leftSentinel
    rightSentinel->left=leftSentinel;
// cout<<leftSentinel->right;
// cout<<"\n";
// cout<<rightSentinel->left;
// cout<<"\n";

  }

  void PushLeft(int val)
  {
    __transaction_atomic{

    
    QNode *qn = (QNode*) malloc(sizeof(QNode));
    qn->val=val;
//    cout<<"value of new node \n";
//    cout<<qn->val;
//    cout<<"\n";
    //there is no any node in between left and right Sentinel
    if(leftSentinel->right==rightSentinel){
 //     cout<<"inside empty node \n";
      leftSentinel->right=qn;
      qn->left=leftSentinel;
      qn->right=rightSentinel;
      rightSentinel->left=qn;
      //cout<<"left sentienel pointed \n";
      //cout<<leftSentinel->right->val;
      //cout<<"\n";
    }
    
    //when there is node in between left and right sentinel
    else{
      //cout<<"inside non-empty node \n";
      QNode *oldleftMostNode=leftSentinel->right;
      leftSentinel->right=qn;
      oldleftMostNode->left=qn;
      qn->left=leftSentinel;
      qn->right=oldleftMostNode;
      //cout<<"left sentienel pointed to \n";
      //cout<<leftSentinel->right->val;
    }
    }
  }

  void PushRight(int val)
  {
    __transaction_atomic{
    QNode *qn = (QNode*) malloc(sizeof(QNode));
    qn->val=val;
  //  cout<<"created new node with new value \n";
    //cout<<qn->val;
    //there is no any node in between left and right Sentinel
    if(leftSentinel->right==rightSentinel){
      //cout<<"empty nodes between \n";

      leftSentinel->right=qn;
      qn->left=leftSentinel;
      qn->right=rightSentinel;
      rightSentinel->left=qn;
      //cout<<"right sentinel pointed to \n";
      //cout<<rightSentinel->left->val;
      //cout<<"\n";

    }
    else{
      //cout<<"inside non-empty node\n";
      QNode *oldrightMostNode=rightSentinel->left;
      oldrightMostNode->right=qn;
      qn->left=oldrightMostNode;
      qn->right=rightSentinel;
      rightSentinel->left=qn;
      //cout<<"right sentinel pointed to \n";
      //cout<<rightSentinel->left->val;
      //cout<<"\n";
      //cout<<"left sentinel pointed to \n";
      //cout<<leftSentinel->right->val;
      }
    }
  }

  int PopLeft()
  {
      __transaction_atomic {

    //if there is no any QNode in between rightSentinel and left Sentinel
      if(leftSentinel->right==rightSentinel){
      return -1;
    }
    //when there is atleast Qnode between rightSentinel and left Sentinel
    else{
      QNode *oldleftMostNode=leftSentinel->right;
      leftSentinel->right=oldleftMostNode->right;
      oldleftMostNode->right->left=leftSentinel;
      return oldleftMostNode->val;
    }
    }
  }

  int PopRight()
  {
  __transaction_atomic {

    //if there is no any QNode in between rightSentinel and left Sentinel
      if(leftSentinel->right==rightSentinel){
      return -1;
    }
    //when there is atleast Qnode between rightSentinel and left Sentinel
    else{
      QNode *oldrightMostNode=rightSentinel->left;
      oldrightMostNode->left->right=rightSentinel;
      rightSentinel->left=oldrightMostNode->left;
      return oldrightMostNode->val;
    }
  }
  }

};

int main()
{
  int num_threads = 2;
  // create and join threads
  std::thread *t = new std::thread[num_threads];
  DQueue d;
  for (int i = 0; i < num_threads; ++i)
  {
    // call void calcArea(int tn, int startidx, int num_trapezes_per_thread, double step)
    //t[i] = std::thread(calcArea, i, num_threads, num_trapezes, step);
 t[i] = std::thread(&DQueue::PushLeft,d,4);
  }

  for (int i = 0; i < num_threads; ++i)
  {
    t[i].join();
  }
  d.PushRight(2);
  d.PushRight(3);
  d.PushRight(4);
  d.PushLeft(5);
  cout << "pop left with value \n";
  cout << d.PopLeft();
  cout << "\n";
  cout << "pop right with value\n";
  cout << d.PopRight();
  cout << "\n";
  cout << "pop right with value\n";
  cout << d.PopRight();
  return 0;
}