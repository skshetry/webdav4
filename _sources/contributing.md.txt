# Contributing Guide

Thank you for taking an interest in the project.
🙏

Please see below on how to setup/test and work on this project.

## Setup
Install all the requirements with:
```shell
pip install -e ".[dev]"
```

## Tests
You can run all the tests with `pytest` command.
The code changes are expected to have tests.

### Test Servers
`webdav4` also provides a test server, that it uses in the `tests` itself.

It can be run with the following commands:
```shell
python -m tests.server
```

You can also run it in an interactive python session with `-i/--interactive` flag:
```shell
python -m tests.server -i
```
This will open a `python` session and provides a few variables like `fs`,
`client`, `server_address`, and `storage_dir` which are also available
for use in `pytest` as fixtures.

To use a real server, you can setup a [Nextcloud server using docker-compose](https://github.com/nextcloud/docker#running-this-image-with-docker-compose)
or an [ownCloud server using docker-compose](https://doc.owncloud.com/server/admin_manual/installation/docker/#docker-compose).

> Note that the webdav url to use will be something like:
> http://{url}/remote.php/dav/files/{username}.
> For example, for me, running in `localhost:8080` and as user `user1`, it is:
> `http://localhost:8080/remote.php/dav/files/user1`.

## Formatting
Please install `pre-commit` and add the hooks, so that it enforces proper
code standard in your code changes.
