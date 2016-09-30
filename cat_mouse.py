from dflow import DFlow
from dflow import dflow, step
from dflow import dflow_result
from dflow import addarg
import argparse
import time
from multiprocessing import freeze_support



@dflow

class CatMouse(DFlow):
    @step()
    def cat(x):
        say = "cat: meow"
        for i in range( 0 , 5):
            print(say)
            time.sleep(2)
        return(say)

    @addarg('-mouse_call',
            default='squeaks',
            help='-mouse_call sound. default: squeaks')
    @step()
    def mouse(x):
        #say = "%s" %(DFlow.args.mouse_call)
        say = "shhhh"

        for i  in range(0,5):
            print(say)
            time.sleep(1)
        print(DFlow.args.mouse_call)
        return(DFlow.args.mouse_call)

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