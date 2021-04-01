#pragma once
#include <string>
#include <string.h>
#include <vector>
#include <iostream>

namespace reverser {

    //the reverser does not support invalid form.

typedef unsigned short ush;
using std::vector;
using std::string;
using std::cout;
using std::endl;

inline vector<string> split_by_comma(const std::string& s) {
    int st = 0;
    int sz = s.size();
    while (st < sz && s[st] != '(') ++st;
    vector<string> ret;
    for (int i = st + 1; i < sz - 1; ++i) {
        int ed = i;
        string cur = "";
        int ctr = 0;
        while (ed < sz - 1 && (ctr || s[ed] != ',')) {
            cur += s[ed];
            if (s[ed] == '(') ++ctr;
            else if (s[ed] == ')') --ctr;
            ed++;
        }
        ret.push_back(cur);
        i = ed;
    }
    return std::move(ret);
}

inline ush get_header_type(const std::string& predicate) {
    if (predicate[0] == 'M' && predicate[1] == 'A') return 0;
    else if (predicate[0] == 'M' && predicate[1] == 'I') return 1;
    else if (predicate[0] == 'K') return 2;
    else if (predicate[0] == '$') return 3;
    return ush(-1);
}

inline int get_kth_num(const string& predicate) {
    int st = 9, ed = st;
    int sz = predicate.size();
    int ret = 0;
    while (ed < sz - 1 && predicate[ed] != ',') {
        ret *= 10;
        ret += predicate[ed] - '0';
        ed++;
    }
    return ret;
}

string dfs_do(const std::string& predicate) {
    int type = get_header_type(predicate);
    if (type == -1)
        throw std::runtime_error("Reverser: something wrong with the format of the predicate");
    if (type == 3) return predicate; 
    vector<string> childs = std::move(split_by_comma(predicate));
    string ret = "";
    int sz = childs.size();
    if (type == 0) {
        ret += "MIN(";
        for (int i = 0; i < sz; ++i) {
            ret += dfs_do(childs[i]);
            if (i != sz - 1) ret += ',';
        }
        ret += ')';
    } else if (type == 1) {
        ret += "MAX(";
        for (int i = 0; i < sz; ++i) {
            ret += dfs_do(childs[i]);
            if (i != sz - 1) ret += ',';
        }
        ret += ')';
    } else if (type == 2) {
        ret += "KTH_MIN($";
        int num = get_kth_num(predicate);
        ret += std::to_string(sz - num);
        ret += ',';
        for (int i = 1; i < sz; ++i) {
            ret += dfs_do(childs[i]);
            if (i != sz - 1) ret += ',';
        }
        ret += ')';
    }
    return std::move(ret);
}

string get_inverse_predicate(const std::string& predicate) {
    return std::move(dfs_do(predicate));
}

}