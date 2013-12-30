#!/usr/bin/env python
"""
Small package to pull meta-data (i.e. url's, description)
about the first 500 images from the British Library's
public image collection. Places meta-data into a redis
database using hash structures prepended by "flickr_"
followed by Flickr's ID for the image.

Hash structures are tracked by an ordered set (in order
based on APIs order when searching BL's collection)
called "flickr".
"""

__author__ = "Aryeh Hillman <aryehbh@gmail.com>"
__version__ = "0.1"
__date__ = "December 30, 2013"

import flickr
import redis
import threading
import Queue
import time
from xml.parsers.expat import ExpatError
import sys
import itertools

# Set up the Flickr API
flickr.debug = False
flickr.API_KEY = 'YOUR_API_KEY_HERE'
# Save the British Library's NSID
BRITISH_LIBRARY_NSID = '12403504@N02'

# Set up connection to Redis
REDIS_HANDLE = redis.StrictRedis(host='localhost', port=6379, db=0)

# Other globals
IMAGE_PREFIX_REDIS = "flickr_"
IMAGE_SET_NAME_REDIS = "flickr"

def get_original_data(image):
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

def get_description(image):
    """Simply wraps a call to the image's
    description attribute. We need this because
    occassionally the flickr package raises
    an error when XML parsing which we want to catch
    to continue processing."""
    try:
        description = image.description
        return description
    except ExpatError:
        #print "Couldn't get description for image %s" % image.id
        sys.stdout.write("_")
        sys.stdout.flush()
    return None

def get_photo_dictionary(photo):
    """Creates a Python dictionary representing
    a photo and returns it. Keys include URLs for
    square, thumbnail, small, medium, large, and original
    sizes; width and height; title; id; and description."""
    original_data = get_original_data(photo)
    description = get_description(photo)
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

def photo_dictionary_into_redis(photo_dictionary):
    """Takes a dictionary representation of a photo
    and inserts it into redis under keyname
    of the photo's id (from the dictionary) prepended
    by the global IMAGE_PREFIX_REDIS string."""
    # Add the photo dictionary to redis
    photo_id = photo_dictionary.get('photo_id')
    name = ''.join([IMAGE_PREFIX_REDIS, photo_id])
    REDIS_HANDLE.hmset(name, photo_dictionary)
    # Add a reference to the photo
    # to an ordered set in redis
    REDIS_HANDLE.zadd(IMAGE_SET_NAME_REDIS, photo_id, name)
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
            photo_dictionary = get_photo_dictionary(photo)
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
            photo_dictionary_into_redis(photo_dictionary)
            sys.stdout.write('o')
            sys.stdout.flush()
            self.photo_dictionary_queue.task_done()

def populate_redis():
    """Main function of this module. Gets the first
    500 images from the British Library's public
    photo collection and stores information about each
    photo including URLs, title, and description in Redis."""

    # Get a sense of how many pages of results we are dealing with
    # num_pages = int(flickr.photos_search_pages(
    #                     user_id=BRITISH_LIBRARY_NSID, per_page=500))

    photo_queue = Queue.Queue()
    photo_dictionary_queue = Queue.Queue()

    print "Getting image pages..."
    for page_number in xrange(1, 2):
        photos = flickr.photos_search(user_id=BRITISH_LIBRARY_NSID,
                                     per_page=500,
                                     page=page_number)
        # Populate the queue with photos
        print "Populating our queue of photos for page %d..." % page_number
        for photo in photos:
            photo_queue.put(photo)

    print "Creating pool of threads to process photos..."
    # Create a pool of threads to process photos
    for _ in itertools.repeat(None, 200):
        pdt = PhotoDictionaryThread(photo_queue, photo_dictionary_queue)
        pdt.setDaemon(True)
        pdt.start()

    print "Creating pool of threads to put photos into redis..."
    # Create a pool of threads to place photos into redis
    for _ in itertools.repeat(None, 20):
        pdr = PhotoDictionaryRedisThread(photo_dictionary_queue)
        pdr.setDaemon(True)
        pdr.start()

    # Wait on the queues
    print "Waiting for the queues to empty!"
    photo_queue.join()
    photo_dictionary_queue.join()

def timeit():
    """Calls method to populate redis with images
    and times the process"""
    start = time.time()
    populate_redis()
    print "Elapsed Time: %s" % (time.time() - start)

if __name__ == '__main__':
    timeit()
