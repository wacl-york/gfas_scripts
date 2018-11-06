#!/usr/bin/env python
"""=============================================================================
TRANSFER GFAS DATA
--------------------------------------------------------------------------------
============================================================================="""
import argparse
import os
import os.path
import sys

import paramiko as pm
#===============================================================================
def get_script_args():
    """
    Get command line arguments and options.
    """
    description = 'Upload preprocessed GFAS file to webfiles for Harvard.'
    arg_parser = argparse.ArgumentParser(description=description)
    arg_parser.add_argument('data_file', metavar='data_file', nargs=1, type=str,
                            help='File to upload to webfiles')
    return arg_parser.parse_args()

def check_input_file(filename):
    """
    Check state of input file before working on it.
    """
    if not (os.path.isfile(filename) and os.access(filename, os.R_OK)):
        raise IOError(f'ERROR: COULD NOT OPEN INPUT FILE {filename}!\n')

def import_key():
    """
    Import RSA key for authentication to webfiles.
    """
    try:
        key_path = os.path.expanduser('~/.ssh/id_rsa')
        key = pm.RSAKey.from_private_key_file(key_path)
    except IOError:
        sys.stderr.write(f'ERROR: COULD NOT OPEN KEY FILE {key_path}!\n')
        exit(1)

    return key

def get_sftp_client(key):
    """
    Create SFTP client, connect, and return transport object.
    """
    host = 'webfiles.york.ac.uk'
    port = 22

    transport = pm.Transport((host, port))
    try:
        transport.connect(username='chem631', pkey=key)
    except pm.SSHException:
        sys.stderr.write(f'ERROR: UNABLE TO ESTABLISH SSH CONNECTION!\n')
        exit(1)

    sftp_client = pm.SFTPClient.from_transport(transport)
    return sftp_client

def push_data_file(client, filename):
    """
    Push preprocessed GFAS data file to webfiles.
    """
    remote_dir = '/var/www/webfiles.york.ac.uk/WACL/GFAS/INCOMING'
    remote_filename = os.path.join(remote_dir, filename)
    attributes = client.put(filename, remote_filename)
    return attributes

def main():
    """
    Main entry point for this script.
    """
    script_args = get_script_args()
    check_input_file(script_args.data_file[0])
    client_key = import_key()
    sftp_client = get_sftp_client(client_key)
    push_data_file(sftp_client, script_args.data_file[0])
    sftp_client.close()
#===============================================================================
if __name__ == '__main__':
    main()
