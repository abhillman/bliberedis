Project in early stage. Uses Flickr's API to populate Redis KV-store hashes
(and ordered set) with information about images in the British Library's 
collection of out-of-copyright images. Access to the API is multithreaded
to facilitate multiple open connections at once for speed.

Execution notes:
- Be sure to get and set an API key from flickr within populate.py
- Only gets the first 500 images...
- Requirements include
    - flickr.py (included!)
    - running instance of redis
    - redis python package
    - simplejson

Bugs are:
- Some errors are not caught (i.e. IOError from socket)
- Sometimes program does not terminate if there is an uncaught error

Other TODO items:
- Look at optimizing API access... currently using existing flickr.py, non-multithreaded
