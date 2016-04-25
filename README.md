# dflow


[Continuum Dask Scheduler](http://dask.pydata.org/en/latest/custom-graphs.html) tells a story of building task flow as a directed acyclical Graph (DAG).  Dflow is an attempt to make such work flows with class.

-----

Dflow is the butler for [Dask](http://dask.pydata.org), the DAG task scheduler. It's designed and developed by @wenglee to provide a pythonic interface to model such flow using OOP class design. It does so by furnishing 3 decorators.

```python
from dflow import DFlow
from dflow import dflow, step
from dflow import dflow_setup
from dflow import addarg
import argparse

@dflow
class CatMouse(DFlow):
	@step()
	def cat(x):
		say = "cat: meow"
		return(say)
	@addarg('-mouse_call', default = 'squeaks',
	help='-mouse_call sound. default: squeaks')

	@step()
	def mouse(x):
		say "%s" %(DFlow.args.mouse_call)
		return(say)

	@step('cat','mouse')
	def play(x):
		print("%s" % dflow_result(x,'cat'))
		print("%s" % dflow_result(x,'mouse'))
if __name__ == "__main__":
	parser = argparse.ArgumentParser(
		description=__name__,
		parents=[DFlow.default_argparse()])

	args = parser.parse_args()
	CatMouse.run(args)
```

Three decorators have been introduced:

* @dflow: class decorator to force @step and @addarg being executed at compile time.
* @step: suggests a class method is a step, and its' corresponding starting condition
* @addarg: suggest additional arguments needed at corresponding  class method.

dflow_result is a helper function to cleanly reads result from results returned from prior steps.

On command line, -step and -phase have been introduced.
-step [list of steps comman separated] does what its' name suggests.
-phase [step]  runs corresponding predecessor steps also.
-run_mode [sync,async]  sync to simplify effort to debug to have tasks run in sequence.

## Contents

- [Install](#install)
- [QuickStart](#quickstart)
- [Development](#development)
- [Author](#author)
- [License](#license)


## Install

### 1. Install dependencies

dflow is built on Continuum's Dask project. Before getting started, you'll need to install dask:

```bash
$ pip install dask
```

```bash
$ git clone github.com/wenglee/dflow
```


## QuickStart

Sample command line run.

To get help on lists of arguments

```bash
$ cat_mouse.py --help
usage: cat_mouse.py [-h] [-mouse_call sound.  Defalt: squeaks.] [-run_mode {async, sync}] [-step] [-phase]

```

To get a list of steps available

```bash
$ python cat_mouse.py -step no_such_step
no_such_step not specifiedin CatMouse.
Valid methods are:
	play,
	mouse,
	cat
```


## Development


- `master` for development.  **All pull requests should be to submitted against `master`.**


## Author

**Weng Lee**
- <https://github.com/wenglee>


## License

Open sourced under the [MIT license](LICENSE.md).

