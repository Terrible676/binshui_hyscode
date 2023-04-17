# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 15:27:11 2023

@author: MC
"""

import hashlib

message = 'Hello, World.'
hash_obj = hashlib.md5()
hash_obj.update(message.encode())
hash_value = hash_obj.hexdigest()
print(hash_value)


# 创建SHA256哈希对象
hash_obj = hashlib.sha256()
hash_obj.update(message.encode())
hash_value = hash_obj.hexdigest()
print(hash_value)



'''
import hashlib
def hash_parameter(param):
    sha256 = hashlib.sha256()
    sha256.update(param.encode('utf-8'))
    return sha256.hexdigest()
password = "my_password"
encrypted_password = hash_parameter(password)
print("Encrypted password: {}".format(encrypted_password))
'''