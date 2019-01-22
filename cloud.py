import os
import time
import hashlib
from shutil import copyfile


from threading import Thread
from glob import glob
from datetime import timedelta, datetime




root_path = 'cloud'




class FileTree:
    
    def generate_file_md5(self, path, blocksize=2**20):
        m = hashlib.md5()
        with open(path, "rb" ) as f:
            while True:
                buf = f.read(blocksize)
                if not buf:
                    break
                m.update(buf)
        return m.hexdigest()
    
    def get_tree(self, root_path=None, exclude=None):
        if root_path is None:
            root_path = self.root_path
#         root_path_len = len(root_path.split(os.path.sep)[-1]) + 1
        paths = []
        for path, dirs, files in os.walk(root_path):
            path = os.path.relpath(path, root_path)
#             path = path[root_path_len:]
            for file in files:
                paths.append(os.path.join(path, file))
            if len(files) == 0:
                paths.append(path)
        
        tree = []
        for path in paths:
            md5 = None
            if os.path.isfile(os.path.join(root_path, path)):
                md5 = self.generate_file_md5(os.path.join(root_path, path))
            tree.append({
                'path': path,
                'time': os.path.getctime(os.path.join(root_path, path)),
                'md5': md5
            })
        return tree
    

    def get_new(self, tree_new=None, tree_old=None):
        if tree_old is None:
            tree_old = self.tree
            
        if tree_new is None:
            tree_new = self.get_tree(self.root_path)
            
        diff = []
        
        for item_loc in tree_new: # current stage
            found = False
            # search path in last stage
            for item in tree_old: # last stage
                if item['path'] == item_loc['path']:
                    found = True
                    if item_loc['time'] > item['time']:
                        if item_loc['md5'] != item['md5']:
                            diff.append(item_loc)
                    break
            # if path not found in stage -> new file
            if not found:
                diff.append(item_loc)
        return diff
    
    
    def get_del(self, tree_new=None, tree_old=None):
        if tree_old is None:
            tree_old = self.tree
            
        if tree_new is None:
            tree_new = self.get_tree(self.root_path)
            
        diff = []
        
        for item in tree_old: # last stage
            found = False
            for item_loc in tree_new: # current stage
                if item['path'] == item_loc['path']:
                    found = True
                    break
            # if path not found in stage -> new file
            if not found:
                diff.append(item)
        
        return diff
    
    def update(self, tree=None):
        if tree is None:
            self.tree = self.get_tree(root_path)
    
    def __init__(self, root_path):
        self.root_path = root_path
        
        if not os.path.exists(self.root_path):
            os.makedirs(self.root_path)
        self.tree = self.get_tree(self.root_path)


file_tree = FileTree(root_path)

file_tree.tree



# class Stage(Thread):
    
#     def __init__(self, root_path, update_seconds=1, synced_dir=None):
#         super(Stage, self).__init__()
        
#         self.file_tree = FileTree(root_path)
        
#         self.update_seconds = update_seconds
#         self.last_update_time = datetime.utcnow()
        
#     def run(self):
#         print('start')
#         while True:
#             if self.last_update_time < datetime.utcnow() + timedelta(seconds=-self.update_seconds):
#                 self.last_update_time = datetime.utcnow()
                
#                 diff = self.file_tree.get_new()
#                 if len(diff)>0:
#                     print('updated files')
#                     print(diff)
#                     if synced_dir is not None:
#                         self.copy(diff, synced_dir)
                        
#                 deleted = self.file_tree.get_del()
#                 if len(deleted)>0:
#                     print('deleted files')
#                     print(deleted)
#                     self.delete(deleted)
                    
#                 self.file_tree.update()
#             time.sleep(1)
            
#     def delete(self, paths):
        
#         # remove files
#         for path in paths:
#             if path['md5'] is not None:
#                 if os.path.exists(path['path']):
#                     os.remove(path['path'])
#         # remove dirs
#         for path in paths:
#             if path['md5'] is None:
#                 if os.path.exists(path['path']):
#                     os.rmdir(path['path'])
                    
#     def copy(self, paths, base_path):
        
#         # copy dirs
#         for path in paths:
#             if path['md5'] is None:
#                 if os.path.exists(path['path']):
#                     os.mkdir(path['path'])
                    
#         # copy files
#         for path in paths:
#             if path['md5'] is not None:
#                 copyfile(os.path.join(base_path, path['path']), path['path'])



class Stage: #(Thread):
    
    def __init__(self, root_path, update_seconds=2, synced_dir=None):
#         super(Stage, self).__init__()
        
        self.file_tree = FileTree(root_path)
        
        self.update_seconds = update_seconds
        self.last_update_time = datetime.utcnow()
        self.synced_dir = synced_dir
        
    def run(self):
        print('start')
        while True:
            if self.last_update_time < datetime.utcnow() + timedelta(seconds=-self.update_seconds):
                self.last_update_time = datetime.utcnow()
                
                remote = self.file_tree.get_tree(self.synced_dir)
                
                
                diff = self.file_tree.get_new(tree_new=remote)
                if len(diff)>0:
                    print('updated files')
                    print(diff)
                    if self.synced_dir is not None:
                        self.copy(diff, self.synced_dir)
                        
                deleted = self.file_tree.get_del(tree_new=remote)
                if len(deleted)>0:
                    print('deleted files')
                    print(deleted)
                    self.delete(deleted)
                    
                self.file_tree.update()
            
            time.sleep(1)
            
    def delete(self, paths):
        root_path = self.file_tree.root_path
        paths = sorted(paths, key=lambda x: -len(x['path']))
        
        # remove files
        for path in paths:
            if path['md5'] is not None:
                if os.path.exists(os.path.join(root_path, path['path'])):
                    print('remove, ', os.path.join(root_path, path['path']))
                    os.remove(os.path.join(root_path, path['path']))
        # remove dirs
        for path in paths:
            if path['md5'] is None:
                if os.path.exists(os.path.join(root_path, path['path'])):
                    # check if not root path
                    if os.path.abspath(os.path.join(root_path, path['path'])) != os.path.abspath(root_path):
                        print('remove, ', os.path.join(root_path, path['path']))
                        os.rmdir(os.path.join(root_path, path['path']))
                    
    def copy(self, paths, base_path):
        root_path = self.file_tree.root_path
        paths = sorted(paths, key=lambda x: len(x['path']))
        # copy dirs
        for path in paths:
            if path['md5'] is None:
                if not os.path.exists(os.path.join(root_path, path['path'])):
                    print('copy, ', os.path.join(root_path, path['path']))
                    os.mkdir(os.path.join(root_path, path['path']))
                    
        # copy files
        for path in paths:
            if path['md5'] is not None:
                print('copy, ', os.path.join(root_path, path['path']))
                copyfile(os.path.join(base_path, path['path']), os.path.join(root_path, path['path']))



stage = Stage(root_path, synced_dir='/home/meikuam/develop/cloud/dir')
stage.run()