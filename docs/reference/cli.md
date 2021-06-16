# `dav` CLI

If `webdav4` is installed with `fsspec` package, `dav` CLI similar to `aws s3` is
also available to make it easier for the interaction.

If not, you can install it as follows:
```shell
pip install -U "webdav4[fsspec]"
```

## Common arguments
- `--endpoint-url`: Path to the server/hostname. Can also be specified through
  `WEBDAV_ENDPOINT_URL` environment variable (and the url can contain password and user information as well).

    ```shell
    export WEBDAV_ENDPOINT_URL="https://user:password@webdav.server.org/remote.php/dav/files/user"
    ```
- `--user`: (Optional) Provide username of the account.
    Can also be specified in the url itself in `--endpoint-url` or through environment variable.
- `--password`: (Optional) Provide password for the account.
      Can also be specified in the url itself in `--endpoint-url` or through environment variable.
- `--verbose`: Provides more information and traceback on error.
- `--help`: Shows help message and exits.

> Note that all the arguments are expected to follow after `dav` command.
> Example: `dav --user user ls`.

## Examples:

**Listing files**

`dav ls`

**Listing files recursively**

`dav ls -R`

**Listing files recursively to certain depth**

`dav ls -R -L2`

**Download a file or a directory from remote**

`dav cp dav://path/to/file file`
`dav cp -R dav://path/to/file file`

**Upload a file or a directory to remote**

`dav cp file dav://path/to/file`
`dav cp -R dir dav://path/to/dir`

**Copy a file or a directory within remote**

`dav cp dav://path/to/file dav://path/to/file2`
`dav cp -R dav://path/to/dir1 dav://path/to/dir2`

> Note: `mv` works similarly to `cp` in both upload
> and download cases, as it does not remove files,
> except when between remotes.

**Move a file or a directory within remote**

`dav mv dav://path/to/file dav://path/to/file2`
`dav mv -R dav://path/to/dir1 dav://path/to/dir2`

**Delete a file or a directory**

`dav rm dav://path/to/file`
`dav rm -R dav://path/to/dir`
`dav rm -R path/dir`

**Create a directory**

`dav mkdir path/dir`
`dav mkdir dav://path/dir`

**Count files and determine total size**

`dav du path/dir`

**Print contents of a file**

`dav cat dav://path/to/file`

**Periodically sync files**

Similar to `aws s3 sync` and uses file sizes and modified times
to sync intelligently between local-remote, remote-local and local-remote.

```shell
dav sync dir dav://path/to/dir  # local to remote
dav sync dav://path/to/dir dir  # remote to local
dav sync dav://path/dir1 dav://path/dir2  # remote-remote
```

**Run multiple commands at once**

```shell

dav run commands.txt  # or,
cat commands.txt | dav run -
```

`commands.txt` can look something like follows:
```
cp dir/dir2 dav://path/to/dir
mv dav://path/dir2 dav://path/dir3
# list all files (comments are supported)
ls -R
du
```
