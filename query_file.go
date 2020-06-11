package mys3

import (
	"fmt"
	"io/ioutil"
	"os"
)

type QueryFile struct {
	Query                string
	Separator, NewLine   string
	Bucket               string
	Keys                 []string
	ACL                  string
	ContentType          string
	ContentEncoding      string
	ServerSideEncryption string
	Zipped               bool
	StrContent           string
}

func (qf *QueryFile) ReadQueryFromFile(path string, sprintfArgs []interface{}) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	b, err := ioutil.ReadAll(file)

	if err != nil {
		return err
	}
	qf.Query = fmt.Sprintf(string(b), sprintfArgs...)

	return nil

}
