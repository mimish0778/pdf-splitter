import os
import sys
import tempfile
from pathlib import Path

# app は読み込み時に作業ディレクトリを決めるので、import より前に一時ディレクトリを指定する
os.environ["WORK_ROOT"] = tempfile.mkdtemp(prefix="pdf-splitter-test-")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
