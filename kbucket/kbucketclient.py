import urllib
import json
import os
import random
import hashlib
import requests
from pairio import client as pairio

class KBucketClient():
  def __init__(self):
    self._config=dict(
        share_ids=[], # remote kbucket shares to search for files
        url=os.getenv('KBUCKET_URL','https://kbucket.flatironinstitute.org'), # the kbucket hub url
        local=True, # whether to search locally by default
        remote=False # whether to search remotely by default
    )
    self._sha1_cache=Sha1Cache()

  def setConfig(self,*,share_ids=None,url=None,local=None,remote=None):
    if share_ids is not None:
      self._config['share_ids']=share_ids
    if url is not None:
      self._config['url']=url
    if local is not None:
      self._config['local']=local
    if remote is not None:
      self._config['remote']=remote

  def findFile(self,path=None,*,sha1=None,share_id=None):
    path, sha1 = self._find_file_helper(path=path,sha1=sha1,share_id=share_id)
    return path

  def realizeFile(self,path=None,*,sha1=None,share_id=None):
    path, sha1 = self._find_file_helper(path=path,sha1=sha1,share_id=share_id)
    if not path:
      return None
    if not _is_url(path):
      return path
    return self._sha1_cache.downloadFile(url=path,sha1=sha1)

  def moveFileToCache(self,path):
    return self._sha1_cache.moveFileToCache(path)

  def readDir(self,path):
    if path.startswith('kbucket://'):
      list=path.split('/')
      share_id=_filter_share_id(list[2])
      path0='/'.join(list[3:])
      obj=self._read_kbucket_dir(share_id=share_id,path=path0)
      if not obj:
        return None
      ret=KBucketClientDirectory()
      for a in obj['files']:
        ff=KBucketClientDirectoryFile()
        ff.name=a['name']
        ff.size=a['size']
        ff.path=path+'/'+ff.name
        ff.sha1=a['prv']['original_checksum']
        ret.files.append(ff)
      for a in obj['dirs']:
        ff=KBucketClientDirectoryDir()
        ff.name=a['name']
        ff.path=path+'/'+ff.name
        ret.dirs.append(ff)
      return ret
    else:
      ret=KBucketClientDirectory()
      list=os.listdir(path)
      for fname in list:
        if os.path.isfile(fname):
          ff=KBucketClientDirectoryFile()
          ff.name=fname
          ff.path=path+'/'+ff.name
          ff.size=os.path.getsize(ff.path)
          ff.sha1=None
          ret.files.append(ff)
        elif os.path.isdir(fname):
          ff=KBucketClientDirectoryDir()
          ff.name=fname
          ff.path=path+'/'+ff.name
          ret.dirs.append(ff)
      return ret

  def computeFileSha1(self,path):
    if path.startswith('sha1://'):
      list=path.split('/')
      sha1=list[2]
      return sha1
    elif path.startswith('kbucket://'):
      path, sha1 = self._find_file_helper(path=path,sha1=None,share_id=None)
      return sha1
    else:
      return self._sha1_cache.computeFileSha1(path)

  def _find_file_helper(self,*,path,sha1,share_id):
    search_remote=self._config['remote']
    if path is not None:
      if sha1 is not None:
        raise Exception('Cannot specify both path and sha1 in find file')

      if path.startswith('sha1://'):
        list=path.split('/')
        sha1=list[2]
        ### continue to below
      elif path.startswith('kbucket://'):
        if share_id is not None:
          raise Exception('Cannot specify both kbucket path and share_id in findFile')
        list=path.split('/')
        share_id=_filter_share_id(list[2])
        path0='/'.join(list[3:])
        prv=self._get_prv_for_file(share_id=share_id,path=path0)
        if not prv:
          return (None, None)
        sha1=prv['original_checksum']
        search_remote=True
        ### continue to below
      else:
        if os.path.exists(path): ## Todo: also check if it is file
          return (path, None)
        else:
          return (None, None)
  
    if self._config['local']:
      path=self._sha1_cache.findFile(sha1=sha1)
      if path:
        return (path,sha1)

    if search_remote:
      all_share_ids=[]
      if share_id is not None:
        all_share_ids=[share_id]
      else:
        all_share_ids=self._config['share_ids']
      for id in all_share_ids:
        url=self._find_in_share(sha1=sha1,share_id=id)
        if url:
          return (url,sha1)
      return (None,None)

    return (None,None)

  def _get_prv_for_file(self,*,share_id,path):
    url=self._config['url']+'/'+share_id+'/prv/'+path
    try:
      obj=_http_get_json(url)
    except:
      return None
    return obj

  def _find_in_share(self,*,sha1,share_id):
    url=self._config['url']+'/'+share_id+'/api/find/'+sha1
    obj=_http_get_json(url)
    if not obj['success']:
      raise Exception('Error finding file in share: '+obj['error'])
    if not obj['found']:
      return None
    urls0=obj['urls']
    for url0 in urls0:
      if _test_url_accessible(url0):
        return url0
    return None

  def _read_kbucket_dir(self,*,share_id,path):
    url=self._config['url']+'/'+share_id+'/api/readdir/'+path
    obj=_http_get_json(url)
    if not obj['success']:
      return None
    return obj

class KBucketClientDirectory:
  def __init__(self):
    self.files=[]
    self.dirs=[]
  def toDict(self):
    ret=dict(
      files=[],
      dirs=[]
    )
    for file in self.files:
      ret['files'].append(file.toDict())
    for dir in self.dirs:
      ret['dirs'].append(dir.toDict())
    return ret

class KBucketClientDirectoryFile:
  def __init__(self):
    self.name=''
    self.path=''
    self.size=None
    self.sha1=None
  def toDict(self):
    return dict(
      name=self.name,
      size=self.size,
      sha1=self.sha1
    )

class KBucketClientDirectoryDir:
  def __init__(self):
    self.name=''
    self.path=''
  def toDict(self):
    return dict(
      name=self.name
    )

def _http_get_json(url):
  return json.load(urllib.request.urlopen(url))

def _test_url_accessible(url):
  try:
    code=urllib.request.urlopen(url).getcode()
    return (code==200)
  except:
    return False

def _is_url(path):
  return ((path.startswith('http://')) or (path.startswith('https://')))

def _filter_share_id(id):
  if '.' in id:
    list=id.split('.')
    if len(list)!=2:
      return id
    return pairio.get(list[1],collection=list[0])
  else:
    return id

# TODO: implement cleanup() for Sha1Cache
# removing .report.json and .hints.json files that are no longer relevant
class Sha1Cache():
  def __init__(self):
    self._directory=os.getenv('SHA1_CACHE_DIR','/tmp/sha1-cache')
  def setDirectory(self,directory):
    self._directory=directory
  def findFile(self,sha1):
    path=self._get_path(sha1,create=False)
    if os.path.exists(path):
      return path
    hints_fname=path+'.hints.json'
    if os.path.exists(hints_fname):
      hints=_read_json_file(hints_fname)
      files=hints['files']
      matching_files=[]
      for file in files:
        path0=file['stat']['path']
        if os.path.exists(path0) and os.path.isfile(path0):
          stat_obj0=_get_stat_object(path0)
          if stat_obj0:
            if (_stat_objects_match(stat_obj0,file['stat'])):
              to_return=path0
              matching_files.append(file)
      if len(matching_files)>0:
        hints['files']=matching_files
        _write_json_file(hints,hints_fname)
        return matching_files[0]['stat']['path']
      else:
        os.remove(hints_fname)

  def downloadFile(self,url,sha1):
    path=self._get_path(sha1,create=True)
    path_tmp=path+'.downloading'
    print('Downloading file: {} -> {}'.format(url,path))
    sha1b=self._download_and_compute_sha1(url,path_tmp)
    if not sha1b:
      if os.exists(path_tmp):
        os.remove(path_tmp)
    if sha1!=sha1b:
      if os.exists(path_tmp):
        os.remove(path_tmp)
      raise Exception('sha1 of downloaded file does not match expected {} {}'.format(url,sha1))
    if os.path.exists(path):
      os.remove(path)
    os.rename(path_tmp,path)
    return path

  def moveFileToCache(self,path):
    sha1=self.computeFileSha1(path)
    path0=self._get_path(sha1,create=True)
    if os.path.exists(path0):
      if path!=path0:
        os.remove(path)
    else:
      os.rename(path,path0)
    return path0

  def computeFileSha1(self,path):
    aa=_get_stat_object(path)
    aa_hash=_compute_string_sha1(json.dumps(aa, sort_keys=True))

    path0=self._get_path(aa_hash,create=True)+'.record.json'
    if os.path.exists(path0):
      obj=_read_json_file(path0)
      bb=obj['stat']
      if _stat_objects_match(aa,bb):
        return obj['sha1']
    sha1=_compute_file_sha1(path)
    obj=dict(
      sha1=sha1,
      stat=aa
    )
    _write_json_file(obj,path0)

    path1=self._get_path(sha1,create=True)+'.hints.json'
    if os.path.exists(path1):
      hints=_read_json_file(path1)
    else:
      hints={'files':[]}
    hints['files'].append(obj)
    _write_json_file(hints,path1)
    ## todo: use hints for findFile
    return sha1

  def _get_path(self,sha1,*,create):
    path0=self._directory+'/{}/{}{}'.format(sha1[0],sha1[1],sha1[2])
    if create:
      if not os.path.exists(path0):
        os.makedirs(path0)
    return path0+'/'+sha1
  def _download_and_compute_sha1(self,url,path):
    hh = hashlib.sha1()
    response=requests.get(url,stream=True)
    path_tmp=path+'.'+_random_string(6)
    with open(path_tmp,'wb') as f:
      for chunk in response.iter_content(chunk_size=512):
        if chunk:  # filter out keep-alive new chunks
          hh.update(chunk)
          f.write(chunk)
    os.rename(path_tmp,path)
    return hh.hexdigest()

def _compute_file_sha1(path):
  if (os.path.getsize(path)>1024*1024*100):
    print('Computing sha1 of {}'.format(path))
  BLOCKSIZE = 65536
  sha = hashlib.sha1()
  with open(path, 'rb') as file:
      buf = file.read(BLOCKSIZE)
      while len(buf) > 0:
          sha.update(buf)
          buf = file.read(BLOCKSIZE)
  return sha.hexdigest()

def _get_stat_object(fname):
  try:
    stat0=os.stat(fname)
    obj=dict(
        path=fname,
        size=stat0.st_size,
        ino=stat0.st_ino,
        mtime=stat0.st_mtime,
        ctime=stat0.st_ctime
    )
    return obj
  except:
    return None

def _stat_objects_match(aa,bb):
  str1=json.dumps(aa, sort_keys=True)
  str2=json.dumps(bb, sort_keys=True)
  return (str1==str2)

def _compute_string_sha1(txt):
  hash_object = hashlib.sha1(txt.encode('utf-8'))
  return hash_object.hexdigest()

def _read_json_file(path):
  with open(path) as f:
    return json.load(f)

def _write_json_file(obj,path):
  with open(path,'w') as f:
    return json.dump(obj,f)

def _random_string(num_chars):
  chars='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
  return ''.join(random.choice(chars) for _ in range(num_chars))

# The global module client
client=KBucketClient()
