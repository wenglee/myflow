from functools import wraps
from dask.multiprocessing import get
import time
import pytz
import datetime
import logging
import os
from logging.handlers import TimedRotatingFileHandler
import signal
import argparse
import smtplib as smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from os.path import basename
from email.utils import COMMASPACE, formatdate
import traceback
import types
import pandas as pd
import sys
import dask
import platform
from multiprocessing import freeze_support

class DFlow():
    _dag = {}
    args = None
    starttime = None
    endtime = None
    steps_result = []
    _args = {}

    @classmethod
    def run(cls, args=args):
        freeze_support()
        DFlow.args = args
        cls.args = args
        rl = logging.getLogger()
        rl.setLevel(logging.DEBUG)
        streamHandler = logging.StreamHandler()
        streamHandler.setLevel(logging.INFO)
        rl.addHandler(streamHandler)

        lh = _setup_loghandler(
            cls.__name__.lower(),
            cls=cls,
            level=logging.INFO)
        rl.addHandler(lh)

        # email_file_name = cls.__name__.lower() + '_email_'\
        #  + str(time.time()) + str(os.getpid())
        # eh = _setup_loghandler(
        # 	email_file_name, cls= cls,
        # 	level = logging.INFO,
        # 	filehandlertype = 'email',
        # 	formatter = None)
        # rl.addHandler(eh)

        if hasattr(args, 'step') and (args.step is not None):
            args.step = ''.join(args.step)
        cls.args = args
        if args is None:
            logging.error("Need to pass in args to DFlow.run(args)")
            os.sys.exit(-1)
        else:
            phase = args.phase
            step = args.step

        def _invalid_step(s):
            str = "%s not specified in %s.\n" \
                  % (s, cls.__name__)
            str += "Valid methods are: \n\t%s." \
                   % ",\n\t".join(cls._dag.keys())
            logging.error(str)
            os.sys.exit(-1)

        cls.starttime = datetime.datetime.utcnow(). \
            strftime("%Y-%m-%d %H:%M%S")
        logging.info("#### RUNARG ####")
        for arg in vars(cls.args):
            if getattr(args, arg) is not None:
                logging.info("RUNARG  %-26s %s"
                             % (arg, getattr(args, arg)))
        logging.info("################")

        if step is not None:
            for s in step.split(','):
                s.strip()
                if (s not in cls._dag) or \
                        (not callable(getattr(cls, s))):
                    _invalid_step(s)
            for s in step.split(','):
                s.strip()
                rl = logging.getLogger()
                m = _setup_loghandler(
                    cls.__name__.lower() + '_' + s.lower(),
                    cls=cls)
                rl.addHandler(m)

                ts = time.time()
                now = datetime.datetime.utcnow()

                logging.info('Running %s' % s)
                try:
                    result = getattr(cls, s)(args)
                except Exception as e:
                    logging.error("""\
						\n##################\
						\n\nException: %s\
						\nwhen running: %s\n\
						\n##################"""
                                  % (traceback.print_exc(), s))
                    # _send_email(
                    # 	subject = "[ERROR]: %s - %s"
                    # 		% (cls.__name__ ,
                    # 		str(cls.args.data_date)),
                    # 	mail_to = cls.args.email_failure,
                    # 	textfile = eh.baseFilename
                    # )
                    # _delete_email_file(eh.baseFilename)
                    return (-1)
                else:
                    logging.debug('Result %s:%s' % (s, result))
                te = time.time()
                logging.info('status2: %s %s - %2.2f min' % \
                             (now, s, (te - ts) / 60))
                cls.steps_result.append({
                    'step': s,
                    'result': result,
                    'duration': (te - ts) / 60,
                    'start': ts,
                    'end': te})
                rl.removeHandler(m)
        elif phase is not None:
            for p in phase:
                if p in cls._dag:
                    result = None
                    try:
                        result = dask.async.get_sync(cls._dag, p) if platform.system() == 'Windows' else get(cls._dag,p)
                    except(Exception, KeyboardInterrupt) as e:
                        logging.error(
                            "%s: %s" % (e, traceback.print_exc()))
                        # _send_email(
                        # 	subject = "[ERROR]: %s - %s"
                        # 	 % (cls.__name__, str(cls.args.data_date)),
                        # 	mail_to = cls.args.email_failure,
                        # 	textfile = eh.baseFilename
                        # )
                        # _delete_email_file(eh.baseFilename)
                        return (-1)
                    cls.steps_result = result
                else:
                    _invalid_step(phase)
        else:
            logging.critical("running all steps. no -step or -phase defined.")
            result = None
            try:
                result = dask.async.get_sync(cls._dag, list(cls._dag.keys())) if platform.system() == 'Windows' \
                    else get(cls._dag, list(cls._dag.keys()))
            except(Exception, KeyboardInterrupt) as e:
                logging.error("%s" % e)
                # _send_email(
                # 		subject = "[ERROR]: %s - %s"
                # 		 % (cls.__name__, str(cls.args.data_date)),
                # 		mail_to = cls.args.email_failure,
                # 		textfile = eh.baseFilename
                # 	)
                # _delete_email_file(eh.baseFilename)
                return (-1)
            cls.steps_result = result

        cls.endtime = datetime.datetime. \
            utcnow().strftime("%Y-%m-%d %H:%M:%S")

        if cls.steps_result is not None:
            logging.info("##### SUMMARY ####")
            summary_result = []
            for step in cls.steps_result:
                if isinstance(step, list):
                    for s in step:
                        summary_result.append(
                            "RUNTIME	%-26s  %.2f min"
                            % (s['step'], s['duration']))
                else:
                    logging.info(
                        "RUNTIME	%-26s  %.2f min"
                        % (step['step'], step['duration']))
            if len(summary_result) > 0:
                list_unique = [s for s in set(summary_result)]
                for s in list_unique:
                    logging.info("%s" % s)
            logging.info("#################")

            rl.removeHandler(lh)
        # rl.removeHandler(eh)

        # _send_email(
        # 	subject = "[SUCCESS]: %s - %s"
        # 		% ( cls.__name__,
        # 			str(cls.args.data_date)),
        # 	mail_to = cls.args.email_success,
        # 	textfile = eh.baseFilename
        # 	)
        # _delete_email_file(eh.baseFilename)

    @staticmethod
    def default_argparse():
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument(
            '-step', default=None, nargs='+',
            help='-step to_run (,)')
        parser.add_argument(
            '-phase', default=None, nargs='+',
            help='-phase to_run (,)')
        parser.add_argument(
            '-logdir', default=None,
            help='-logdir fullpath for logdir. default: .')

        def _is_odd(i):
            return bool(i & 1)

        _seen_method_arg = {}
        for method in DFlow._args.keys():
            i = 0
            while i < len(DFlow._args[method]):
                if (_is_odd(i)):
                    arg = DFlow._args[method][i - 1][0]
                    if _seen_method_arg.get(arg, 0):
                        print("WARN: Overwriting default arg %s in %s as defined in %s"
                              % (arg, method, _seen_method_arg.get(arg, 0)))
                    # parser.set_defaults(arg = DFlow._args[method][i]['default'])
                    else:
                        _seen_method_arg[arg] = method
                        parser.add_argument(
                            arg,
                            **DFlow._args[method][i])
                i += 1

        return parser


# Class decorator
def dflow(cls):
    for methodname in dir(cls):
        method = getattr(cls, methodname)
        if callable(method) and \
                ('__' not in methodname) \
                and hasattr(method, '_prop'):
            cls._dag.update(
                {methodname:
                     (_mytimeit(cls, method),
                      method._prop)})
        if callable(method) and \
                ('__' not in methodname) \
                and hasattr(method, '_args'):
            cls._args.update(
                {methodname: (method._args)})

    return cls


def step(*args):
    def _wrapper(func):
        func._prop = None
        if type(args) is list:
            func._prop = args
        elif len(args) == 1:
            func._prop = args[0]
        elif len(args) > 1:
            func._prop = list(args)
        return func

    return _wrapper


def addarg(*args, **kw):
    def _wrapper(func):
        if hasattr(func, '_args'):
            func._args += (args, kw)
            # if platform.system() == 'Windows':
            #     setattr(DFlow.args,args[0].replace('-',''), kw['default'])
        else:
            func._args = (args, kw)
            # if platform.system() == 'Windows':
            #     DFlow.args = lambda: None
            #     setattr(DFlow.args, args[0].replace('-',''), kw['default'])
        return func

    return _wrapper


def _mytimeit(cls, method, *args, **kw):
    # wrapper func
    def timer(*args, **kw):
        rl = logging.getLogger()
        m = _setup_loghandler(
            cls.__name__.lower() + '.' + \
            method.__name__.lower(),
            cls=cls)
        rl.addHandler(m)

        ts = time.time()
        now = datetime.datetime.utcnow()

        logging.info('Running %s %s'
                     % (now, method.__name__))
        result = None
        try:
            # result = getattr(cls,s)(args)
            result = method(*args, **kw)
        except Exception as e:
            logging.error("""\
				\n##################\
				\n\nException: %s\
				\nwhen running: %s\n\
				\n##################"""
                          % (traceback.print_exc(),
                             method.__name__))
            raise e
        except KeyboardInterrupt as e:
            logging.error("""\
				\n##################\
				\n\nException: KeyboardInterrupt\
				\nwhen running: %s\n\
				\n##################"""
                          % (method.__name__))
            raise e
        else:
            logging.debug('Result %s: %s' \
                          % (method.__name__, result))

        te = time.time()

        logging.info('status1: %s %s - %2.2f min' % \
                     (now, method.__name__, (te - ts) / 60))
        rl.removeHandler(m)

        r = []
        if args[0] is not None:
            if (sys.version_info > (3, 1) and
                    (isinstance(result, (list, tuple))) and
                    (sys.version_info <= (3.1) and
                         isinstance(args[0], types.GeneratorType))):
                for x in args[0]:
                    if type(x) is not dict:
                        for y in x:
                            r.append(y)
                    else:
                        r.append(x)
        else:
            # make args[0] unique by step
            try:
                r = list({v['step']: v for v in args[0]}.values())
            except Exception as e:
                # raise ValueError(
                #     "Some results from predcessor of step %s is not defined: %s"
                #     % (method.__name__, e))
                # sort list by start
                r = sorted(r, key=lambda k: k['start'])
        r.append({
            'step': method.__name__,
            'result': result,
            'duration': (te - ts) / 60,
            'start': ts,
            'end': te
        })
        cls.steps_result = r
        return r

    return timer


def _setup_loghandler(
        logger_name, log_file=None,
        cls=None, level=logging.DEBUG,
        formatter=logging.Formatter(
            "[%(asctime)s - %(levelname)s] %(message)s"),
        filehandlertype=None):
    if log_file is None:
        if hasattr(DFlow.args, 'logdir') and \
                (DFlow.args.logdir is not None):
            log_file = os.path.join(
                DFlow.args.logdir, logger_name) + '.log'
        else:
            log_file = os.path.join('.', logger_name) + '.log'

    if filehandlertype == 'email':
        fileHandler = logging.FileHandler(log_file)
    else:
        fileHandler = TimedRotatingFileHandler(
            log_file,
            when='H',
            interval=10,
            backupCount=10)
    fileHandler.setLevel(level)
    if formatter is not None:
        fileHandler.setFormatter(formatter)
    return fileHandler


def dflow_result(
        result_array,
        curstep=None,
        return_type='result'
):
    if curstep is None:
        raise ValueError('step is not specified')
    else:
        def _log_dataframe(r):
            if (isinstance(r[return_type], pd.DataFrame)):
                logging.debug(
                    "%s (%s row[s]): %s" % (
                        curstep,
                        len(r[return_type].index),
                        r[return_type].info()
                    ))

        if isinstance(result_array, argparse.Namespace):
            result_array = DFlow.steps_result
        for result in result_array:
            if sys.version_info > (3, 1):
                if (isinstance(result, (list, tuple))):
                    if result[0]['step'] == curstep:
                        _log_dataframe(result[0])
                        return (result[0][return_type])
                else:
                    if result['step'] == curstep:
                        _log_dataframe(result)
                        return (result[return_type])
            else:
                if (isinstance(result, types.GeneratorType)):
                    if result[0]['step'] == curstep:
                        _log_dataframe(result[0])
                        return (result[0][return_type])
                else:
                    if result['step'] == curstep:
                        _log_dataframe(result)
                        return (result[return_type])
        raise ValueError("no result for %s" % curstep)


def _send_email(
        subject=None,
        mail_server='mailhub.xyz.com',
        mail_from='dev.null@xyz.com',
        mail_to=None,
        textfile=None):
    if os.getenv("NOEMAIL", "not found") != "not found":
        mail_to = None
        logging.warn("NOT SENDING EMAIL: %s" % subject)
    elif os.getenv("ALLEMAILSTO", "not found") != "not found":
        mail_to = os.getenv("ALLEMAILSTO")
        logging.warn("SENDING EMAIL to ALLEMAILSTO env: %s"
                     % mail_to)
    if mail_to is None:
        return True

    with open(textfile) as fp:
        msg = MIMEText(fp.read())

    if subject is None:
        msg['Subject'] = 'The contents of %s' % textfile
    else:
        msg['Subject'] = subject
    msg['From'] = mail_from
    msg['To'] = mail_to

    s = smptplib.SMTP(mail_server)
    s.send_message(msg)
    s.quit()


def _delete_email_file(file=None):
    try:
        os.remove(file)
    except OSError:
        pass
