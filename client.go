package mys3

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"database/sql"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type Client struct {
	session  *session.Session
	db       *sql.DB
	region   string
	bucket   string
	uploader *s3manager.Uploader
}

func GetClient(s3Region string, db *sql.DB) (*Client, error) {

	c := Client{region: s3Region}

	s, err := session.NewSession(&aws.Config{Region: aws.String(s3Region)})

	if err != nil {
		return nil, err
	}

	c.uploader = s3manager.NewUploader(s)

	c.session = s

	c.db = db

	return &c, nil

}

func (c *Client) Close() error {
	return c.db.Close()
}

func (c Client) QueryToS3(qf QueryFile, omitEmpty bool) error {
	var w io.Writer
	var buffer bytes.Buffer

	if qf.Zipped {
		w = gzip.NewWriter(&buffer)
		defer w.(*gzip.Writer).Close()

	} else {
		w = bufio.NewWriter(&buffer)
		defer w.(*bufio.Writer).Flush()

	}

	rows, err := c.db.Query(qf.Query)
	if err != nil {
		return err
	}
	defer rows.Close()

	headers, err := rows.Columns()

	if err != nil {
		return err
	}

	lh := len(headers)

	for _, v := range headers[:lh-1] {
		_, err = io.WriteString(w, v)
		if err != nil {
			return err
		}
		_, err = io.WriteString(w, qf.Separator)
		if err != nil {
			return err
		}

	}

	_, err = io.WriteString(w, headers[lh-1])
	if err != nil {
		return err
	}
	_, err = io.WriteString(w, qf.NewLine)
	if err != nil {
		return err
	}

	nullRow := make([]sql.NullString, lh)
	aux := make([]interface{}, lh)

	for i := range nullRow {
		nullRow[i] = sql.NullString{}
		aux[i] = &nullRow[i]
	}

	rc := 0
	for rows.Next() {
		rc++
		err := rows.Scan(aux...)
		if err != nil {
			return err
		}
		for _, v := range nullRow[:lh-1] {
			if v.Valid {
				_, err = io.WriteString(w, v.String)
				if err != nil {
					return err
				}
			}
			_, err = io.WriteString(w, qf.Separator)
			if err != nil {
				return err
			}

		}

		if nullRow[lh-1].Valid {
			_, err = io.WriteString(w, nullRow[lh-1].String)
			if err != nil {
				return err
			}
		}

		_, err = io.WriteString(w, qf.NewLine)
		if err != nil {
			return err
		}

	}

	if rc == 0 && omitEmpty {
		return nil
	}

	return c.upload(qf, buffer)

}

func (c Client) StringToS3(qf QueryFile) error {
	var w io.Writer
	var buffer bytes.Buffer

	if qf.Zipped {
		w = gzip.NewWriter(&buffer)
		defer w.(*gzip.Writer).Close()

	} else {
		w = bufio.NewWriter(&buffer)
		defer w.(*bufio.Writer).Flush()

	}

	io.WriteString(w, qf.StrContent)

	return c.upload(qf, buffer)
}

func (c Client) upload(qf QueryFile, buffer bytes.Buffer) error {
	var nb bytes.Buffer

	ref := &nb
	tee := io.TeeReader(&buffer, ref)
	for _, key := range qf.Keys {

		_, err := c.uploader.Upload(&s3manager.UploadInput{
			Bucket:               aws.String(qf.Bucket),
			Key:                  aws.String(key),
			ACL:                  aws.String(qf.ACL),
			Body:                 tee,
			ContentType:          aws.String(qf.ContentType),
			ContentEncoding:      aws.String(qf.ContentEncoding),
			ServerSideEncryption: aws.String(qf.ServerSideEncryption),
		})
		if err != nil {
			return err
		}
		var cnb bytes.Buffer
		tee = io.TeeReader(ref, &cnb)
		ref = &cnb
	}
	return nil
}
