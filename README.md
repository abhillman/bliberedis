Project in early stage. Uses Flickr's API to populate Redis KV-store hashes
(and ordered set) with information about images in the British Library's 
collection of out-of-copyright images. Access to the API is multithreaded
to facilitate multiple open connections at once for speed.

Bugs are:
- Some errors are not caught (i.e. IOError from socket)
- Sometimes program does not terminate if there is an uncaught error

Other TODO items:
- Look at optimizing API access... currently using existing flickr.py, non- multithreaded
