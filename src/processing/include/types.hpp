#ifndef CMSC_TYPES_HPP
#define CMSC_TYPES_HPP

enum SchemaType { ST_TEXT, ST_INT, ST_BOOL };

template <typename T>
concept TRedditData = requires(T t) {
    // called before any processing
    t.reset();
    // called after processing, return determines if we write entry to database
    { t.valid() } -> std::convertible_to<bool>;
};

template <TRedditData T>
using SchemaCallback = std::function<void(const T&, std::string&)>;

template <TRedditData T>
struct SchemaDef {
    const std::string key;
    const SchemaType type;
    const SchemaCallback<T> callback;

    SchemaDef(const std::string& key, const SchemaType type, const SchemaCallback<T>& callback) : key(key), type(type), callback(callback) {}
    SchemaDef(const SchemaDef& a) : key(a.key), type(a.type), callback(a.callback) {}
    SchemaDef() {}
};

template <TRedditData T>
struct Schema {
private:
    // cols is for table creating, head is for inserting
    // will be in same order
    std::string cols;
    std::string head;

    void init() {
        std::stringstream _cols;
        std::stringstream _head;

        int i = 0;
        int m = def.size() - 1;
        for (const auto& s : def) {
            switch (s.type) {
                case ST_TEXT:
                    _cols << s.key + " TEXT";
                    break;
                case ST_INT:
                    _cols << s.key + " INTEGER";
                    break;
                case ST_BOOL:
                    _cols << s.key + " INTEGER CHECK (" << s.key << " IN (0,1))";
                    break;
            }

            _head << s.key;

            if (i++ != m) {
                _head << ",";
                _cols << ",";
            }
        }

        cols = _cols.str();
        head = _head.str();
    }
public:
    const std::string name;
    const std::vector<SchemaDef<T>> def;

    // Schema(const std::string& table, const std::vector<SchemaDef<T>>& def) : name(table), def(def) { init(); }
    Schema(const std::string table, const std::vector<SchemaDef<T>>& def) : name(table), def(def) { init(); }
    Schema(const Schema& a) : name(a.name), def(a.def) { init(); }
    Schema() {}

    const std::string& columns() const { return cols; }
    const std::string& columns_ins() const { return head; }
};

#endif