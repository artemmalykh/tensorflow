/* Copyright 2018 The TensorFlow Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#ifndef TF_IGFS_UTILS_H
#define TF_IGFS_UTILS_H

#include <iostream>
#include <map>
#include <memory>

using namespace std;

class Reader {
 public:
  Reader(istream* is) : is(is) {
    pos = 0;
  }

  Reader() : Reader(&cin) {

  }

  void printEverything() {
    std::cout << "=======Printing everything=======" << std::endl;
    char c = 0;
    do {
      *is >>  c;
      std::cout << c << '\t';
    } while(c != std::streambuf::traits_type::eof());
    std::cout << std::endl;
  }

  int skip(int n) {
    is->ignore(n);

    pos += n;

    return n;
  };

  int skipToPos(int pos) {
    int toSkip = std::max(0, pos - this->pos);
    is->ignore(toSkip);

    this->pos += toSkip;

    return toSkip;
  };

  // 4 bytes
  int readInt() {
    char buf[4];
    is->read(buf, 4);

    pos += 4;

    return (buf[0] << 24) |
           (buf[1] << 16) |
           (buf[2] << 8) |
           (buf[3]);
  }

  // 4 bytes
  long readLong() {
    char buf[8];
    is->read(buf, 8);

    pos += 8;

    return ((0xff & buf[0]) << 56) |
           ((0xff & buf[1]) << 48) |
           ((0xff & buf[2]) << 40) |
           ((0xff & buf[3]) << 32) |
           ((0xff & buf[4]) << 24) |
           ((0xff & buf[5]) << 16) |
           ((0xff & buf[6]) << 8) |
           ((0xff & buf[7]));
  }

  void readBytes(char* dest, int len) {
    is->read(dest, len);
  }

  // 2 bytes
  short readShort() {
    char buf[2];
    is->read(buf, 2);

    pos += 2;

    return (buf[0] << 8) | (buf[1]);
  }

  char readChar() {
    char d = 0;
    *is >> d;

    pos += 1;

    return d;
  }

  bool readBool() {
    char d = 0;
    *is >> d;

    pos += 1;

    return d != 0;
  }

  string readNullableString() {
    bool isEmpty = readBool();

    if (isEmpty) {
      return string();
    } else {
      return readString();
    }
  }

  map<string, string> readStringMap() {
    int size = readInt();

    map<string, string> res = map<string, string>();


    for (int i = 0; i < size; i++) {
      res.insert(std::pair<string, string>(readString(), readString()));
    }

    return res;
  };
  
  string readString() {
    short len = readShort();
    auto *cStr = new char[len + 1];
    cStr[len] = 0;
    is->read(cStr, len);

    pos += len;

    return string(cStr);
  }

  void reset() {
    pos = 0;
  }

 private:
  // Position to be read next
  int pos;
  unique_ptr<istream> is;
};

class Writer {
 private:
  std::unique_ptr<std::ostream> os;
  int pos;

 public:
  Writer() : os(&std::cout) {

  }

  Writer(std::ostream* os) : os(os) {
    pos = 0;
  }

  void writeShort(unsigned short s) {
    *os <<  (char) ((s >> 8) & 0xFF);
    *os <<  (char) (s & 0xFF);

    pos += 2;
  }

  void writeInt(int s) {
    *os <<  (char) ((s >> 24) & 0xFF)
        << (char) ((s >> 16) & 0xFF)
        << (char) ((s >> 8) & 0xFF)
        << (char) (s & 0xFF);

    pos += 4;
  }

  void writeLong(long s) {
    *os <<  (char) ((s >> 56) & 0xFF)
        << (char) ((s >> 48) & 0xFF)
        << (char) ((s >> 40) & 0xFF)
        << (char) ((s >> 32) & 0xFF)
        << (char) ((s >> 24) & 0xFF)
        << (char) ((s >> 16) & 0xFF)
        << (char) ((s >> 8) & 0xFF)
        << (char) (s & 0xFF);

    pos += 8;
  }

  // TODO: get rid of specifying concrete map parameters
  void writeSize(map<string, string>::size_type s) {
    *os <<  (char) ((s >> 24) & 0xFF)
        << (char) ((s >> 16) & 0xFF)
        << (char) ((s >> 8) & 0xFF)
        << (char) (s & 0xFF);

    pos += 4;
  }

  void writeChar(char c) {
    *os <<  c;

    pos += 1;
  }

  void skip(int n) {
    for (int i = 0; i < n; i++) {
      *os <<  (char) 0;
    }

    pos += n;
  }

  void skipToPos(int n) {
    int toSkip = std::max(0, n - pos);

    for (int i = 0; i < toSkip; i++) {
      *os <<  (char) 0;
    }

    pos += toSkip;
  }

  void writeBoolean(bool val) {
    *os <<  (char) (val ? 1 : 0);

    pos += 1;
  }

  void writeString(string str) {
    if (!str.empty()) {
      writeBoolean(false);
      unsigned short l = str.length();
      writeShort(l);
      *os <<  str;

      int size = 1 + 2 + l;
      pos += size;
    } else {
      writeBoolean(true);
    }
  }

  void writeBytes(const char* bytes, int len) {
    os->write(bytes, len);
  }

  void writeStringMap(map<string, string> map) {
    map<string, string>::size_type size = map.size();
    writeSize(size);

    for (auto const &x : map) {
      writeString(x.first);
      writeString(x.second);
    }
  }

  void flush() {
    os->flush();
    pos = 0;
  }
};

template<class T> class Optional {
 public:
  template<class T1> static Optional<T1> of(T1 val) {
    return Optional(val);
  }

  template<class T1> static Optional<T1> empty() {
    return Optional();
  }

  bool isEmpty() {
    return hasVal;
  }

  T getOrElse(T altVal) {
    return isEmpty() ? altVal : val;
  }

  T get() {
    return val;
  }

  Optional() {
    hasVal = false;
  }

  Optional(T val) {
    this->val = val;
  }

  void read(Reader& r) {
    hasVal = r.readBool();

    if (hasVal) {
      val = T();
      val.read(r);
    }
  }

 private:
  bool hasVal = false;
  T val;
};

#endif //TF_CMAKE_UTILS_H
