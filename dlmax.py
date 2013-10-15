#!/usr/bin/env python

import re
import os
import sys
import time
import pickle
import httplib
import hashlib
import urlparse
import threading

MIN_CHUNK = 10 * 1024 * 1024
MAX_NUM_CHUNKS = 10
COOKIE = ''

httplib.MAXAMOUNT = 4096

class ChunkSet:
  def __init__(self, url):
    self.url = url
    self.length = 0
    self.finished_count = 0
    self.path = '%s.download' % hashlib.md5(url).hexdigest()[:6]

  def log(self, format, *args):
    sys.stdout.write(format % args + '\n')
    sys.stdout.flush()

  def progress(self):
    bytes_so_far = 0
    for chunk in self.chunks:
      bytes_so_far += chunk.bytes_so_far
    return bytes_so_far / float(self.length)

  def start_or_resume(self):
    self.log('downloading %s', self.url)
    try:
      self.resume()
    except IOError:
      self.start()
    self.log('download is %s bytes', self.length)
    self.log('downloading in %d %s', len(self.chunks), 'chunks' if len(self.chunks) != 1 else 'chunk')
    for i, chunk in enumerate(self.chunks):
      self.log('chunk %d from %d to %d', i, chunk.start, chunk.end)

    # Start the downloads
    for chunk in self.chunks:
      threading.Thread(target=chunk.run).start()

    # Show progress
    old_progress = self.progress()
    old_time = time.time()
    average_rate = 0
    while self.finished_count < len(self.chunks):
      time.sleep(1)
      new_progress = self.progress()
      new_time = time.time()
      text = '%.3f%%' % (100 * new_progress)

      rate = (new_progress - old_progress) / (new_time - old_time)
      average_rate = 0.95 * average_rate + 0.05 * rate
      old_progress = new_progress
      old_time = new_time

      text += ' %.2f KB/sec' % (self.length * average_rate / 1024)

      time_left = (1 - new_progress) / average_rate
      hours = time_left // 3600
      time_left -= hours * 3600
      minutes = time_left // 60
      time_left -= 60 * minutes
      if hours:
        text += ' %d ' % hours + ('hour' if hours == 1 else 'hours')
      if minutes:
        text += ' %d ' % minutes + ('minute' if minutes == 1 else 'minutes')
      seconds = int(time_left)
      text += ' %d ' % seconds + ('second' if seconds == 1 else 'seconds')

      sys.stdout.write(text + '        \r')
      sys.stdout.flush()

    # Done
    self.log('download done, assembling file                ')
    match = re.search('/([^/]+)$', self.url)
    path = match.group(1) if match else self.path[:-9]
    f = open(path, 'wb')
    for chunk in self.chunks:
      f.write(open(chunk.path, 'rb').read())
    f.close()
    self.log('saved to %s', path)
    for chunk in self.chunks:
      os.unlink(chunk.path)
    os.unlink(self.path)

  def finished(self, chunk):
    self.finished_count += 1

  def send(self, method, headers):
    parse = urlparse.urlparse(self.url)
    schemes = {
      'http': httplib.HTTPConnection,
      'https': httplib.HTTPSConnection,
    }
    con = schemes[parse.scheme](parse.hostname, parse.port or 80)
    self.log('%s %s %s', method, parse.path, repr(headers))
    con.request(method, parse.path, None, headers)
    res = con.getresponse()
    self.log('%s %s', res.status, res.reason)
    return res

  def start(self):
    self.log('starting download')
    res = self.send('GET', { 'cookie': COOKIE })
    if res.status != 200 or not res.length:
      sys.exit('could not get download length')
    self.length = res.length
    res.fp.close()

    # Set chunk boundaries
    chunk_size = max(MIN_CHUNK, (self.length + MAX_NUM_CHUNKS - 1) // MAX_NUM_CHUNKS)
    num_chunks = (self.length + chunk_size - 1) // chunk_size
    self.chunks = []
    for i in range(num_chunks):
      start = i * chunk_size
      end = min(start + chunk_size, self.length)
      self.chunks.append(Chunk(self, start, end))

    self.save()

  def resume(self):
    data = pickle.load(open(self.path, 'rb'))
    self.url = data['url']
    self.length = data['length']
    self.chunks = []
    for chunk in data['chunks']:
      self.chunks.append(Chunk(self, chunk['start'], chunk['end']))
    self.log('resuming download')

  def save(self):
    data = {
      'url': self.url,
      'length': self.length,
      'chunks': [{ 'start': chunk.start, 'end': chunk.end } for chunk in self.chunks],
    }
    pickle.dump(data, open(self.path, 'wb'))

class Chunk:
  def __init__(self, chunk_set, start, end):
    self.chunk_set = chunk_set
    self.start = start
    self.end = end
    self.bytes_so_far = 0
    self.path = chunk_set.path + '.%d-%d' % (start, end)

  def run(self):
    # Start from where we left off last time
    f = open(self.path, 'ab')
    self.bytes_so_far = os.path.getsize(self.path)

    # Keep downloading until done
    while self.start + self.bytes_so_far < self.end:
      start = self.start + self.bytes_so_far
      end = self.end - 1
      self.chunk_set.log('requesting bytes %d to %d', start, end)
      res = self.chunk_set.send('GET', { 'Range': 'bytes=%d-%d' % (start, end), 'cookie': COOKIE })
      if res.status != 206:
        self.chunk_set.log('server doesn\'t support partial content')
        return

      class FP:
        def __init__(self, chunk, fp):
          self.chunk = chunk
          self.fp = fp

        def read(self, n):
          data = self.fp.read(n)
          f.write(data)
          f.flush()
          self.chunk.bytes_so_far += len(data)
          return data

        def close(self):
          self.fp.close()

      # Download as much as we can this time, but write data to disk immediately
      res.fp = FP(self, res.fp)
      try:
        res.read()
      except httplib.IncompleteRead:
        pass

    f.close()
    self.chunk_set.log('chunk %d to %d finished', self.start, self.end)
    self.chunk_set.finished(self)

# Get the URL to download
if len(sys.argv) != 2:
  sys.exit('\nusage:\n   %s URL\n' % sys.argv[0])
url = sys.argv[1]
ChunkSet(url).start_or_resume()
