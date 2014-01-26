package collections

import (
	"encoding/json"
	"strings"
	"github.com/mortdeus/go9p"
	"github.com/mortdeus/go9p/srv"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type Collections struct {
	srv.File
	uid go9p.User
	db  *mgo.Database
	collections map[string]*srv.File
}

func (c *Collections) Create(fid *srv.FFid, name string, perm uint32) (*srv.File, error) {
	if collection, ok := c.collections[name]; ok {
		return collection, nil
	}
	collection := new(srv.File)
	if err := collection.Add(&c.File, name, c.uid, nil, go9p.DMDIR|0555, collection); err != nil {
		return nil, err
	}
	query := new(Text)
	if err := query.Add(collection, "query", c.uid, nil, 0666, query); err != nil {
		return nil, err
	}
	mmap := new(Text)
	if err := mmap.Add(collection, "map", c.uid, nil, 0666, mmap); err != nil {
		return nil, err
	}
	reduce := new(Text)
	if err := reduce.Add(collection, "reduce", c.uid, nil, 0666, reduce); err != nil {
		return nil, err
	}

	documents := new(Documents)
	if err := documents.Add(collection, "documents", c.uid, nil, 0666, documents); err != nil {
		return nil, err
	}
	documents.collection = c.db.C(name)
	documents.query = query
	documents.mmap = mmap
	documents.reduce = reduce

	c.collections[name] = collection
	return collection, nil
}

func Add(f *srv.File, uid go9p.User, db *mgo.Database) error {
	c := new(Collections)
	c.uid = uid
	c.db  = db

	collections, err := db.CollectionNames()
	if err != nil {
		return err
	}
	c.collections = make(map[string]*srv.File)
	for _, collection := range collections {
		if _, err := c.Create(nil, collection, 0); err != nil {
			return err
		}
	}

	return c.Add(f, "collections", uid, nil, go9p.DMDIR|0777, c)
}

type Text struct {
	srv.File
	data []byte
}

func (t *Text) Open(fid *srv.FFid, mode uint8) error {
	if (mode & go9p.OTRUNC) != 0 {
		t.data = []byte{}
		t.Length = 0
	}
	return nil
}

func (t *Text) Read(fid *srv.FFid, buf []byte, offset uint64) (int, error) {
	t.Lock()
	defer t.Unlock()

	have := len(t.data)
	off := int(offset)

	if off >= have {
		return 0, nil
	}
	return copy(buf, t.data[off:]), nil
}

func (t *Text) Write(fid *srv.FFid, buf []byte, offset uint64) (int, error) {
	t.Lock()
	defer t.Unlock()

	sz := offset + uint64(len(buf))
	if t.Length < sz {
		data := make([]byte, sz)
		copy(data, t.data)
		t.data = data
		t.Length = sz
	}
	if t.Length > sz {
		t.Length = sz
	}
	return copy(t.data[offset:], buf), nil
}

func (t *Text) Data() []byte {
	return t.data[:t.Length]
}

type Documents struct {
	srv.File
	collection *mgo.Collection
	query      *Text
	mmap       *Text
	reduce     *Text
	data       []byte
}

func (d *Documents) Open(fid *srv.FFid, mode uint8) error {
	d.Lock()
	defer d.Unlock()

	q := bson.M{}
	qdata := d.query.Data()
	if len(qdata) > 0 {
		if err := json.Unmarshal(qdata, &q); err != nil {
			return err
		}
	}

	docs := []bson.M{}
	query := d.collection.Find(q)
	m := strings.TrimSpace(string(d.mmap.Data()))
	r := strings.TrimSpace(string(d.reduce.Data()))
	var err error
	if len(m) == 0 && len(r) == 0 {
		err = query.All(&docs)
	} else {
		job := &mgo.MapReduce{
			Map: m,
			Reduce: r,
		}
		_, err = query.MapReduce(job, &docs)
	}
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(docs, "", "\t")
	if err != nil {
		return err
	}
	d.data = data
	d.Length = uint64(len(data))
	return nil
}

func (d *Documents) Read(fid *srv.FFid, buf []byte, offset uint64) (int, error) {
	d.Lock()
	defer d.Unlock()

	have := len(d.data)
	off := int(offset)

	if off >= have {
		return 0, nil
	}
	return copy(buf, d.data[off:]), nil
}

func (d *Documents) Write(fid *srv.FFid, buf []byte, offset uint64) (int, error) {
	d.Lock()
	defer d.Unlock()

	sz := offset + uint64(len(buf))
	if d.Length < sz {
		data := make([]byte, sz)
		copy(data, d.data)
		d.data = data
		d.Length = sz
	}
	count := copy(d.data[offset:], buf)
	if d.Length > sz {
		d.Length = sz
	}

	docs := []bson.M{}
	if err := json.Unmarshal(d.data[:d.Length], &docs); err != nil {
		return count, err
	}
	
	var lasterr error
	for _, doc := range docs {
		iid, ok := doc["_id"]
		if !ok {
			if err := d.collection.Insert(doc); err != nil {
				lasterr = err
			}
			continue
		}
		sid, ok := iid.(string)
		if !ok {
			continue
		}
		delete(doc, "_id")
		oid := bson.ObjectIdHex(sid)
		if err := d.collection.UpdateId(oid, doc); err != nil {
			lasterr = err
		}
	}
	return count, lasterr
}
