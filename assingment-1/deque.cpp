#include <stdio.h> /* printf, scanf, NULL */
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
    leftSentinel = (QNode *)malloc(sizeof(QNode));
    leftSentinel->val = leftSentinelValue;
    leftSentinel->left = NULL;
    rightSentinel = (QNode *)malloc(sizeof(QNode));
    rightSentinel->val = rightSentinelValue;
    rightSentinel->right = NULL;

    //connect leftSentinel Node with rightSentinel
    leftSentinel->right = rightSentinel;
    //connect rightSentinel Node with leftSentinel
    rightSentinel->left = leftSentinel;
  }

  void PushLeft(int val)
  {
    __transaction_atomic
    {
      QNode *qn = (QNode *)malloc(sizeof(QNode));
      qn->val = val;
      //there is no any node in between left and right Sentinel
      if (leftSentinel->right == rightSentinel)
      {
        leftSentinel->right = qn;
        qn->left = leftSentinel;
        qn->right = rightSentinel;
        rightSentinel->left = qn;
      }

      //when there is node in between left and right sentinel
      else
      {
        //cout<<"inside non-empty node \n";
        QNode *oldleftMostNode = leftSentinel->right;
        leftSentinel->right = qn;
        oldleftMostNode->left = qn;
        qn->left = leftSentinel;
        qn->right = oldleftMostNode;
      }
      // cout << "left push new node of value \n";
      // cout << leftSentinel->right->val;
      // cout << "\n";
    }
  }

  void PushRight(int val)
  {
    __transaction_atomic
    {
      QNode *qn = (QNode *)malloc(sizeof(QNode));
      qn->val = val;
      //there is no any node in between left and right Sentinel
      if (leftSentinel->right == rightSentinel)
      {
        //cout<<"empty nodes between \n";

        leftSentinel->right = qn;
        qn->left = leftSentinel;
        qn->right = rightSentinel;
        rightSentinel->left = qn;
      }
      else
      {
        //cout<<"inside non-empty node\n";
        QNode *oldrightMostNode = rightSentinel->left;
        oldrightMostNode->right = qn;
        qn->left = oldrightMostNode;
        qn->right = rightSentinel;
        rightSentinel->left = qn;
      }
      // cout << "right push new node of value \n";
      // cout << rightSentinel->left->val;
      // cout << "\n";
    }
  }

  int PopLeft()
  {
    __transaction_atomic
    {

      //if there is no any QNode in between rightSentinel and left Sentinel
      if (leftSentinel->right == rightSentinel)
      {
        return -1;
      }
      //when there is atleast Qnode between rightSentinel and left Sentinel
      else
      {
        QNode *oldleftMostNode = leftSentinel->right;
        leftSentinel->right = oldleftMostNode->right;
        oldleftMostNode->right->left = leftSentinel;
        return oldleftMostNode->val;
      }
      // cout << "after left pop node of value \n";
      // cout << leftSentinel->right->val;
      // cout << "\n";
    }
  }

  int PopRight()
  {
    __transaction_atomic
    {

      //if there is no any QNode in between rightSentinel and left Sentinel
      if (leftSentinel->right == rightSentinel)
      {
        return -1;
      }
      //when there is atleast Qnode between rightSentinel and left Sentinel
      else
      {
        QNode *oldrightMostNode = rightSentinel->left;
        oldrightMostNode->left->right = rightSentinel;
        rightSentinel->left = oldrightMostNode->left;
        return oldrightMostNode->val;
      }
      // cout << "after right pop we have node of value \n";
      // cout << rightSentinel->left->val;
      // cout << "\n";
    }
  }
};

int main()
{
  int num_threads = 8;
  // create and join threads
  std::thread *t = new std::thread[num_threads];
  DQueue d;
  t[0] = std::thread(&DQueue::PushLeft, d, 3);
  t[1] = std::thread(&DQueue::PushLeft, d, 4);
  t[2] = std::thread(&DQueue::PushLeft, d, 5);
  t[3] = std::thread(&DQueue::PushLeft, d, 6);

  t[4] = std::thread(&DQueue::PopLeft, d);
  t[5] = std::thread(&DQueue::PopRight, d);
  t[6] = std::thread(&DQueue::PopLeft, d);
  t[7] = std::thread(&DQueue::PopRight, d);

  for (int i = 0; i < num_threads; ++i)
  {
    t[i].join();
  }

  return 0;
}