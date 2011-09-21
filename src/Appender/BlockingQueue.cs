using System;
using System.Collections.Generic;
using System.Threading;
using System.Text;


namespace Haukcode.AmqpJsonAppender
{
    public class LossyBlockingQueue<T> : IDisposable
    {
        private Queue<T> _queue = new Queue<T>();
        private Semaphore _semaphore = new Semaphore(0, int.MaxValue);
        private int maxQueueLength;

        public LossyBlockingQueue(int maxQueueLength)
        {
            this.maxQueueLength = maxQueueLength;
        }

        public void Enqueue(T data)
        {
            if (data == null) throw new ArgumentNullException("data");
            bool added = false;
            lock (_queue)
            {
                if (_queue.Count < maxQueueLength)
                {
                    // Only queue if we have less than X items in the queue
                    _queue.Enqueue(data);
                    added = true;
                }
            }
            if(added)
                _semaphore.Release();
        }

        public T Dequeue()
        {
            _semaphore.WaitOne();
            lock (_queue) return _queue.Dequeue();
        }

        void IDisposable.Dispose()
        {
            if (_semaphore != null)
            {
                _semaphore.Close();
                _semaphore = null;
            }
        }
    }
}