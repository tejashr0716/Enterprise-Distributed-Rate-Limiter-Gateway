CXX = clang++
CXXFLAGS = -O3 -Wall -Wextra -std=c++17 -fPIC -Iinclude
LDFLAGS = 

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    SHARED_LIB = libgatekeeper.dylib
    SHARED_FLAGS = -dynamiclib
else
    SHARED_LIB = libgatekeeper.so
    SHARED_FLAGS = -shared
endif

all: $(SHARED_LIB) benchmark_runner

$(SHARED_LIB): src/rate_limiter.cpp include/rate_limiter.h
	$(CXX) $(CXXFLAGS) $(SHARED_FLAGS) src/rate_limiter.cpp -o $(SHARED_LIB)

benchmark_runner: src/main_benchmark.cpp src/rate_limiter.cpp include/rate_limiter.h
	$(CXX) $(CXXFLAGS) src/main_benchmark.cpp src/rate_limiter.cpp -o benchmark_runner

clean:
	rm -f $(SHARED_LIB) benchmark_runner *.o

.PHONY: all clean
