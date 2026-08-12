#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import argparse
import getpass
import sys

from subprocess import Popen, PIPE

if os.getenv('JAVA_HOME') is None:
    print("[W] ---------- JAVA_HOME environment property not defined, using java in path. ----------")
    JAVA_BIN = 'java'
else:
    JAVA_BIN = os.path.join(os.getenv('JAVA_HOME'), 'bin', 'java')
print(f"Using Java:{JAVA_BIN}")


def main():
    parser = argparse.ArgumentParser(description="Manage Ranger JCEKS credential aliases.")
    parser.add_argument("-l", "--libpath", dest="library_path",
                        help="Path to folder where credential libs are present")
    parser.add_argument("-f", "--file", dest="jceks_file_path", help="Path to jceks file to use")
    parser.add_argument("-k", "--key", dest="key", help="Key to use")
    parser.add_argument("-c", "--create", dest="create", help="Add a new alias")
    args = parser.parse_args()

    value = ''
    if args.create:
        value = getpass.getpass("Value: ") if sys.stdin.isatty() else sys.stdin.readline().rstrip("\n")
        getorcreate = 'create'
    else:
        getorcreate = 'get'
    call_keystore(args.library_path, args.jceks_file_path, args.key, value, getorcreate)


def call_keystore(libpath, filepath, aliasKey, aliasValue='', getorcreate='get'):
    finalLibPath = libpath.replace('\\', '/').replace('//', '/')
    finalFilePath = 'jceks://file/' + filepath.replace('\\', '/').replace('//', '/')

    if getorcreate == 'create':
        commandtorun = [JAVA_BIN, '-cp', finalLibPath, 'org.apache.ranger.credentialapi.buildks',
                        'create', aliasKey, '-value', aliasValue, '-provider', finalFilePath]
        p = Popen(commandtorun, stdin=PIPE, stdout=PIPE, stderr=PIPE, text=True)
        output, error = p.communicate()
        if p.returncode == 0:
            print(f"Alias {aliasKey} created successfully!")
        else:
            print(f"Error creating Alias!! Error: {error.strip()}")

    elif getorcreate == 'get':
        commandtorun = [JAVA_BIN, '-cp', finalLibPath, 'org.apache.ranger.credentialapi.buildks',
                        'get', aliasKey, '-provider', finalFilePath]
        p = Popen(commandtorun, stdin=PIPE, stdout=PIPE, stderr=PIPE, text=True)
        output, error = p.communicate()
        if p.returncode == 0:
            print(f"Alias : {aliasKey} Value : {output.strip()}")
        else:
            print(f"Error getting value!! Error: {error.strip()}")

    else:
        print('Invalid Arguments!!')


if __name__ == '__main__':
    main()