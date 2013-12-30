#!/usr/bin/env python
import flickr
import redis
import simplejson
import threading
import Queue
import time
from xml.parsers.expat import ExpatError
import sys

# Set up the Flickr API
flickr.debug = False
flickr.API_KEY = 'YOUR_API_KEY_HERE'
# Save the British Library's NSID
british_library_nsid = '12403504@N02'

# Set up connection to Redis
redis_handle = redis.StrictRedis(host='localhost', port=6379, db=0)

# Other globals
image_prefix_redis = "flickr_"
image_set_name_redis = "flickr"

def getOriginalData(image):
    """Method to get the original
    image url and dimensions from
    a flickr.py image object"""
    try:
        image_sizes = image.getSizes()
        for image_size in image_sizes:
            if image_size.get('label') == 'Original':
                return image_size
    except ExpatError:
        #print "Couldn't get image sizes for image %s" % image.id
        sys.stdout.write("_")
        sys.stdout.flush()
    return {}

def getDescription(image):
    try:
        description = image.description
    except ExpatError:
        #print "Couldn't get description for image %s" % image.id
        sys.stdout.write("_")
        sys.stdout.flush()
    return None

def getPhotoDictionary(photo):
    original_data = getOriginalData(photo)
    description = getDescription(photo)
    photo_dictionary = dict(
            photo_id = photo.id,
            title = photo.title,
            descripton = description,
            square_url = photo.getSmallSquare(),
            thumbnail_url = photo.getThumbnail(),
            small_url = photo.getSmall(),
            medium_url = photo.getMedium(),
            large_url = photo.getLarge(),
            original_url = original_data.get('source'),
            width = original_data.get('width'),
            height = original_data.get('height'),
            )
    return photo_dictionary

def photoDictionaryIntoRedis(photo_dictionary):
    # Add the photo dictionary to redis
    photo_id = photo_dictionary.get('photo_id')
    name = ''.join([image_prefix_redis, photo_id])
    redis_handle.hmset(name, photo_dictionary)
    # Add a reference to the photo
    # to an ordered set in redis
    redis_handle.zadd(image_set_name_redis, photo_id, name)
    return name

class PhotoDictionaryThread(threading.Thread):
    """Photo Dictionary Create"""
    def __init__(self, photo_queue, photo_dictionary_queue):
        threading.Thread.__init__(self)
        self.photo_queue = photo_queue
        self.photo_dictionary_queue = photo_dictionary_queue 
    
    def run(self):
        while True:
            # Get photo from queue
            photo = self.photo_queue.get()
            # Construct dictionary for photo
            photo_dictionary = getPhotoDictionary(photo)
            sys.stdout.write('x')
            sys.stdout.flush()
            self.photo_dictionary_queue.put(photo_dictionary)
            self.photo_queue.task_done()

class PhotoDictionaryRedisThread(threading.Thread):
    """Plop Photo Dictionary into Redis"""
    def __init__(self, photo_dictionary_queue):
        threading.Thread.__init__(self)
        self.photo_dictionary_queue = photo_dictionary_queue
    
    def run(self):
        while True:
            # Get photo dictionary from queue
            photo_dictionary = self.photo_dictionary_queue.get()
            photoDictionaryIntoRedis(photo_dictionary)
            sys.stdout.write('o')
            sys.stdout.flush()
            self.photo_dictionary_queue.task_done()

photo_queue = Queue.Queue()
photo_dictionary_queue = Queue.Queue()
def populate_redis():
    # Get a sense of how many pages of results we are dealing with 
    # num_pages = int(flickr.photos_search_pages(
    #                     user_id=british_library_nsid, per_page=500))
    # We hard code the num_pages here -- don't need to do this
    num_pages = 2040

    #for x in xrange(3, 11):
    print "Getting image pages..."
    for x in xrange(30, 40):
        photos = flickr.photos_search(user_id=british_library_nsid,
                                     per_page=500,
                                     page=x)
        # Populate the queue with photos
        print "Populating our queue of photos for page %d..." % x
        for photo in photos:
            photo_queue.put(photo)

    print "Creating pool of threads to process photos..."
    # Create a pool of threads to process photos
    for i in range(200):
        pdt = PhotoDictionaryThread(photo_queue, photo_dictionary_queue)
        pdt.setDaemon(True)
        pdt.start()

    print "Creating pool of threads to put photos into redis..."
    # Create a pool of threads to place photos into redis
    for i in range(20):
        pdr = PhotoDictionaryRedisThread(photo_dictionary_queue)
        pdr.setDaemon(True)
        pdr.start()
    
    # Wait on the queues
    print "Waiting for the queues to empty!"
    photo_queue.join()
    photo_dictionary_queue.join()

def timeit():
    start = time.time()
    populate_redis()
    print "Elapsed Time: %s" % (time.time() - start)

if __name__=='__main__':
    timeit()
