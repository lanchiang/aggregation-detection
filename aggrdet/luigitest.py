# Created by lan at 2021/3/5
# Linked Luigi Example - Gouthaman Balaraman
# http://gouthamanbalaraman.com/blog/building-luigi-task-pipeline.html

import luigi
from luigi.mock import MockTarget


class SimpleTask(luigi.Task):
    """
    SimpleTask prints Hello World!.
    """

    def output(self):
        return MockTarget("SimpleTask", mirror_on_stderr=True)

    def run(self):
        _out = self.output().open('w')
        _out.write(u"Hello World!\n")
        _out.close()


class DecoratedTask(luigi.Task):
    """
    DecoratedTask depends on the SimpleTask
    """

    def output(self):
        return MockTarget("DecoratedTask", mirror_on_stderr=True)

    def requires(self):
        return SimpleTask()

    def run(self):
        _in = self.input().open("r")
        _out = self.output().open('w')
        for line in _in:
            outval = u"Decorated " + line + u"\n"
            _out.write(outval)

        _out.close()
        _in.close()


if __name__ == '__main__':
    # Modified run would look like
    # luigi.run(["--lock-pid-dir", "D:\\temp\\", "--local-scheduler"], main_task_cls=DecoratedTask)
    luigi.run(["--local-scheduler", '--log-level', 'WARNING'], main_task_cls=DecoratedTask)

