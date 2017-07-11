#!/usr/bin/env python2
# -*- coding: utf-8 -*-

"""
Finds and downloads random images using the Google Custom Search API.
"""

__author__ = 'Adam Rafuse <$(echo nqnz.enshfr#tznvy.pbz | tr a-z# n-za-m@)>'

import os
import sys
import random
import datetime
import time
import argparse
import threading
import signal
import urlparse
import fcntl, termios, struct

import requests

# Configurable API prerequisites (https://developers.google.com/custom-search/json-api/v1/overview)
API_KEY = 'YOUR_API_KEY_HERE'
SEARCH_ENGINE_ID = 'YOUR_SEARCH_ENGINE_ID_HERE'

# Application defaults
NUM_IMAGES = 30             # Number of images to fetch
MAX_IMAGE_YEARS = 10        # For Android phones, oldest images to search for
MAX_IMAGE_FILE = 200        # For other devices, highest image number to search for
IMAGE_SIZE = 'medium'       # Desired image size
IMAGE_FORMAT = 'jpg'        # Desired image file format
QUERY_BATCH_SIZE = 10       # Number of query results to fetch at once (max 10)
QUERY_RETRIES = 3           # Number of query retry attempts before giving up on errors
NUM_THREADS = 4             # Number of worker threads to spawn
DOWNLOAD_CHUNK_SIZE = 1024  # Streaming download chunk size 

# File name templates for common cameras and phones
FILENAME_SIGS = {
    'img' : 'IMG_{:04d}',            # iPhone, Canon G2
    'imgd': 'IMG_{:%Y%m%d_%H%M%S}',  # Android
    'dsc' : 'DSC_{:04d}',            # Nikon D1
    'sbcs': 'SBCS{:04d}',            # Canon EOS
    'dscn': 'DSCN{:04d}',            # Nikon
    'dcp' : 'DCP_{:04d}',            # Kodak DC
    'dscf': 'DSCF{:04d}',            # Fuji Finepix
    'pict': 'PICT{:04d}',            # Minolta
    'mvc' : 'MVC-{:04d}'             # Mavica
}

# Base URL of the Custom Search API
SEARCH_API_BASE = 'https://www.googleapis.com/customsearch/v1'

def main(output, num_images, image_size, image_format, batch_size, workers):    
    random_images = RandomImageLocator(size=image_size, format=image_format)
    downloaders = []

    # Create download workers
    for i in xrange (workers):
        downloaders.append(Downloader(str(i), output))
        downloaders[i].start()
        
    # Start status printer worker
    printer = ThreadStatusPrinter(downloaders)
    printer.start()   
        
    # Allow user interrupt (CTRL+C) to shut down cleanly
    def shutdown_handler(*args, **kwargs):
        printer.footer = "(Got user interrupt, closing ...)"
        for downloader in downloaders:
            downloader.stop()
            downloader.join()  # Let downloads finish before stopping the printer worker
        printer.stop()
        sys.exit(0)
    signal.signal(signal.SIGINT, shutdown_handler)
    
    # Download random images
    try:
        for url in random_images.generate_urls(num_images, batch_size):
            # Get sizes of all worker thread queues
            queue_sizes = []
            for i, downloader in enumerate(downloaders):
                queue_sizes.append(len(downloader.urls))
      
            # Schedule based on min queue size
            i = queue_sizes.index(min(queue_sizes))
            downloaders[i].urls.append(url)
            downloaders[i].resume()
                
    except IOError as e:
        print 'IOError: ' + e   
    
    # Signal threads to stop
    for downloader in downloaders:
        downloader.stop()
    printer.stop()
        
class ThreadStatusPrinter(threading.Thread):
    """
    Handles printing of output and status refreshes for multiple threads.
    """
    
    def __init__(self, thread_objects, *args, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)
        self.thread_objects = thread_objects
        self.footer = None
        self._stopping = False

    def run(self):
        """
        Continually prints the statuses of passed thread objects to stdout. If an object contains a
        'completed_statuses' (str list) property the first elements are popped off and printed once. If an object
        contains a 'status_line' (str) property it is continually refreshed in-place at the bottom of the output. An
        optional 'self.footer' (str) is finally printed at the bottom of the output.
        """
        
        printed_lines = 0
        while not (self._stopping):
            rows, _ = self.terminal_size()
            printed_lines = 0
                    
            # Print any completed statuses first
            for i, obj in enumerate(self.thread_objects):
                while (obj.completed_statuses):
                    string = 'Thread {}: {}'.format(i, obj.completed_statuses.pop(0))
                    sys.stdout.write('{}{}\n'.format(string, ' ' * (rows - len(string))))
            
            # Refresh current statuses
            for i, obj in enumerate(self.thread_objects):
                if (obj.status_line is not None):
                    string = 'Thread {}: {}'.format(i, obj.status_line)
                    sys.stdout.write('{}{}\n'.format(string, ' ' * (rows - len(string))))
                    printed_lines += 1
                    
            # Print footer if it exists
            if (self.footer):
                sys.stdout.write(self.footer + '\n')
                printed_lines += 1
    
            # Back cursor over previous lines for refresh (ANSI CPL escape)
            sys.stdout.write('\033[{}F'.format(printed_lines)) 
            
            time.sleep(0.5)  # TODO: Make this configurable
            sys.stdout.flush()
        
        # Return the cursor to where it should be at the end
        sys.stdout.write('\n' * printed_lines)
    
    def terminal_size(self):
        """
        Portable magic to get the current size of the terminal window in characters. Returns a tuple of (width,
        height).
        
        http://stackoverflow.com/a/3010495
        """
        
        height, width, _, _ = struct.unpack('HHHH',
            fcntl.ioctl(0, termios.TIOCGWINSZ,
            struct.pack('HHHH', 0, 0, 0, 0)))
        return width, height
    
    def stop(self):
        self._stopping = True
        
class Downloader(threading.Thread):
    """
    Multithreaded streaming downloader.
    """
    
    def __init__(self, name, directory, *args, **kwargs):
        threading.Thread.__init__(self, name=name, *args, **kwargs)
        self.urls = []
        self.name = name
        self.directory = directory
        self.status_line = 'Downloading ...'
        self.completed_statuses = []
        self._stopping = False
        self._waiting = False
    
    def run(self):
        """
        Keeps pulling URLs out of self.urls and downloads them to self.directory. Pauses if the URL queue is empty
        until self.resume() is called. Keeps track of the current download status including the transfer rate in
        self.status_line. Adds any completed statuses (done or failed) to self.completed_statuses. Continues until
        self.stop() is called, which will allow the last download to finish. Calling self.interrupt() will cancel any
        download in progress.
        """
        
        while not (self._stopping):
            if not (self._waiting):
                try:
                    url = self.urls.pop(0)
                    filename = urlparse.urlsplit(url).path.split('/')[-1]
                    filepath = os.path.join(self.directory, filename)
                    self.status_line = 'Downloading (... KB/S) {} ...'.format(filepath)
                    
                    try:
                        response = requests.get(url, stream=True)
                        with open(filepath, 'wb') as f:  # TODO: Check for existing files
                            start_time = time.time()
                            current_bytes = 0
                            kbps = 0
                            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                                time_delta = time.time() - start_time
                                
                                self.status_line = 'Downloading ({} KB/s) {} ...'.format(kbps, filepath)
                                                                
                                if chunk:  # Ignore keep-alive chunks
                                    current_bytes += len(chunk)
                                    kbps = int((float(current_bytes) / (time_delta)) / 1024)
                                    f.write(chunk)
                
                        # Download completed
                        self.status_line += ' Done'
                        self.completed_statuses.append(self.status_line)
                
                    except requests.ConnectionError:
                        # Requests has given up retrying the download
                        self.status_line += ' FAILED'
                        self.completed_statuses.append(self.status_line)

                except IndexError:
                    # Empty queue, wait for manual resume to avoid raising more IndexErrors
                    self._waiting = True
                    
        self.status_line += ' Done'

                    
    def stop(self):
        self._stopping = True
        self.urls = []
    
    def is_stopping(self):
        return self._stopping
    
    def resume(self):
        self._waiting = False
    
class RandomImageLocator(object):
    """
    Locates random images using the Google Custom Search API.
    """
    
    def __init__(self, *args, **kwargs):
        self._image_size = kwargs.pop('size', IMAGE_SIZE)
        self._image_format = kwargs.pop('format', IMAGE_FORMAT)
        self._sources = []

    def query_images(self, term, number, start):
        """
        Queries 'number' images using the Google Custom Search API for 'term', and returns results beginning at
        'start'.
        """
        
        parameters = {
            'q': term,
            'cx': SEARCH_ENGINE_ID,
            'fileType': self._image_format,
            'imgSize': self._image_size,
            'number':  number,
            'searchType': 'image',
            'start': start,
            'fields': 'items(displayLink,link)',
            'prettyPrint': 'false',
            'key': API_KEY,
        }
        return requests.get(SEARCH_API_BASE, params=parameters)

    def get_url_batch(self, number):
        """
        Get a batch of 'number' random image URLs.
        """
        
        result = {
            'status': 200,
            'message': 'OK',
            'urls': []
        }
            
        offset = random.randint(1, 100 - number)  # Custom Search API returns only the first 100 results
        response = self.query_images(self.random_camera_filename(), number, offset)
        if (response.status_code == 200):
            try:
                results = response.json()['items']
                for i in xrange (0, len(results)):
                    # Only use URLs from a unique source
                    source = results[i]['displayLink']
                    if (source not in self._sources):
                        self._sources.append(source)
                        result['urls'].append(results[i]['link'])
            except KeyError:
                # Query executed ok but no images were found
                pass
        else: 
            result['status'] = response.status_code
            result['message'] = response.text
    
        return result
    
    def generate_urls(self, number, batch_size):
        """
        Yields up to 'number' image URLs from unique sources. Retries transient errors with exponential backoff.        
        Raises IOError if generation cannot continue due to an error.
        """
        
        total = 0
        retries = 0
        while (total < number):
            response = self.get_url_batch(batch_size)
            if ((response['status'] >= 500 and response['status'] <= 599) or (response['status'] in [400, 408, 429])):
                # Retriable error
                retries += 1;
                if (retries > QUERY_RETRIES):
                    raise IOError("{}, {}".format(response['status'], response['message']))
                    return
                time.sleep(2 ** retries)
            elif (response['status'] == 200):
                # Success
                retries = 0
                for url in response['urls']:
                    yield url
                    total += 1;
            else:
                # Unretriable error
                raise IOError("{}, {}".format(response['status'], response['message']))
                return
    
    @classmethod
    def random_camera_filename(cls):
        """
        Returns a random filename which may exist for an image taken by a number of common cameras / phones.
        """
        
        key = random.choice(FILENAME_SIGS.keys())
        if (key == 'imgd'):
            return FILENAME_SIGS[key].format(cls.random_past_datetime(MAX_IMAGE_YEARS)) + '.' + IMAGE_FORMAT
        else:
            return FILENAME_SIGS[key].format(random.randint(1, MAX_IMAGE_FILE)) + '.' + IMAGE_FORMAT
        
    @classmethod
    def random_past_datetime(cls, years):
        """
        Returns a random datetime between now and 'years' years ago. 
        """
        
        now = datetime.datetime.utcnow()
        then = now.replace(year = now.year - years)
        delta = (now - then).total_seconds()
        return datetime.datetime.fromtimestamp(time.mktime(then.timetuple()) + random.randint(1, delta))
    
if __name__ == '__main__':
    argParser = argparse.ArgumentParser(description="Downloads random images from the internet.")
    argParser.add_argument('output', help="Output directory to save downloaded images to.")
    argParser.add_argument('-n', '--number', type=int,
                           help="Number of images to download (default {}).".format(NUM_IMAGES))
    argParser.add_argument('-s', '--size',
                           help="Desired image size (default {}).".format(IMAGE_SIZE))
    argParser.add_argument('-f', '--format',
                           help="Desired image format (default {}).".format(IMAGE_FORMAT))
    argParser.add_argument('-b', '--batchsize', type=int,
                           help="Query batch size (default {}, max 10).".format(QUERY_BATCH_SIZE))
    argParser.add_argument('-w', '--workers', type=int,
                           help="Number of worker threads to spawn (default {}).".format(NUM_THREADS))
    args = argParser.parse_args()
    
    if not (os.path.isdir(args.output)):
        sys.stderr.write("Directory '{}' not found.\n\n".format(args.output))
    else: 
        main(args.output,
             args.number if args.number else NUM_IMAGES,
             args.size if args.size else IMAGE_SIZE,
             args.format if args.format else IMAGE_FORMAT,
             args.batchsize if args.batchsize else QUERY_BATCH_SIZE,
             args.workers if args.workers else NUM_THREADS)