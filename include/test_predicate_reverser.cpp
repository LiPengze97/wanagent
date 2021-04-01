#include "wan_predicate_reverser.hpp"
using namespace reverser;

using std::cout;
using std::endl;

void test_splitter(const string& predicate) {
    vector<string> V = std::move(split_by_comma(predicate));
    for (int i = 0; i < V.size(); ++i) {
        cout << V[i] << ' ';
    }
    cout << endl;
}

void test_get_kth_num() {
    cout << get_kth_num("KTH_MIN($2)") << endl;
    cout << get_kth_num("KTH_MIN($101010)") << endl;
    cout << get_kth_num("KTH_MIN($214748)") << endl;
    cout << get_kth_num("KTH_MIN($2,MAX($1,$5),MIN($2,$3))") << endl;
}

void test_reverser() {
    cout << get_inverse_predicate("MAX(MAX(MAX($3,$4,$5),$5),MIN(MAX($1,$2,$3),$5),$3,$4)") << endl;
    cout << get_inverse_predicate("KTH_MIN($2,MAX($1,$5),MIN($2,$3))") << endl;
    cout << get_inverse_predicate("KTH_MIN($4,KTH_MIN($2,KTH_MIN($2,$10,$11),$100),MIN($1,$2,$3,$4),MAX(MAX($5,$6),MIN($7,$8)),KTH_MIN($1,$177,$178),MAX($12,$13),KTH_MIN($2,$13,$14),MIN(MAX($1,$2)))") << endl;
}

int main() {
    cout << "TESTING: splitter" << endl;
    test_splitter("MAX(MAX(MAX($3,$4,$5),$5),MIN(MAX($1,$2,$3),$5),$3,$4)");
    test_splitter("KTH_MIN($2,MAX($1,$5),MIN($2,$3))");
    cout << endl;
    cout << "TESTING: get_kth_num" << endl;
    test_get_kth_num();
    cout << endl;
    cout << "TESTING: reverser" << endl;
    test_reverser();
    cout << endl;
    return 0;
}