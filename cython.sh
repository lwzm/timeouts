cython -3 timeouts.py
gcc -shared -pthread -fPIC -fwrapv -O3 -Wall -fno-strict-aliasing \
    -I"$HOME/anaconda3/include/python3.6m" -o timeouts.so timeouts.c
