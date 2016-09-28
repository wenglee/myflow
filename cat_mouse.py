from dflow import DFlow
from dflow import dflow, step
from dflow import dflow_result
from dflow import addarg
import argparse

@dflow
class CatMouse(DFlow):
    @step()
    def cat(x):
        say = "cat: meow"
        return(say)

    @addarg('-mouse_call',
            default='squeaks',
            help='-mouse_call sound. default: squeaks')
    @step()
    def mouse(x):
        say = "%s" %(DFlow.args.mouse_call)
        return(say)

    @step('cat','mouse')
    def play(x):
        print('hi')
        print("%s" % dflow_result(x,'cat'))
        print("%s" % dflow_result(x,'mouse'))
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__name__,
        parents=[DFlow.default_argparse()])

    args = parser.parse_args()
    CatMouse.run(args)