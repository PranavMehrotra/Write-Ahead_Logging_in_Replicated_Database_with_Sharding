import threading

# Class to implement a reader-writer lock, which allows multiple readers to read at the same time, but only one writer
# to write at a time. This is a writer-preference lock, which means that if a writer is waiting to write, no new readers
# will be allowed to read. This is to give quick access to the writers.
class RWLock:
    def __init__(self):
        self.__reader_queue = threading.Lock()
        self.__no_readers = threading.Lock()
        self.__no_writers = threading.Lock()
        
        self.__read_switch = LightSwitch()
        self.__write_switch = LightSwitch()

    def acquire_reader(self):
        self.__reader_queue.acquire()
        self.__no_readers.acquire()
        self.__read_switch.lock(self.__no_writers)
        self.__no_readers.release()
        self.__reader_queue.release()

    def release_reader(self):
        self.__read_switch.unlock(self.__no_writers)

    def acquire_writer(self):
        self.__write_switch.lock(self.__no_readers)
        self.__no_writers.acquire()

    def release_writer(self):
        self.__no_writers.release()
        self.__write_switch.unlock(self.__no_readers)

    def acquired_by_writer(self):
        return self.__write_switch.locked()
    
    def acquired_by_reader(self):
        return self.__read_switch.locked()


# Class to implement a lightswitch, which is a synchronization tool that allows many threads to lock a semaphore/lock,
# where first thread acquires the semaphore/lock and last thread releases the semaphore/lock
class LightSwitch:
    def __init__(self):
        self.counter = 0
        self.mutex = threading.Lock()

    def lock(self, semaphore):
        self.mutex.acquire()
        self.counter += 1
        if self.counter == 1:
            semaphore.acquire()
        self.mutex.release()

    def unlock(self, semaphore):
        self.mutex.acquire()
        self.counter -= 1
        if self.counter == 0:
            semaphore.release()
        self.mutex.release()

    def locked(self):
        return self.counter > 0