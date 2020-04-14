package mys3

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"database/sql"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/go-sql-driver/mysql"
)

type client struct {
	session *session.Session
	db      *sql.DB
	region  string
	bucket  string
}

func Client(region, bucket string, dbUser, dbPassword, dbHost, dbPort, dbName string) (*client, error) {

	c := client{region: region, bucket: bucket}

	s, err := session.NewSession(&aws.Config{Region: aws.String(region)})

	if err != nil {
		return nil, err
	}

	c.session = s

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPassword, dbHost, dbPort, dbName)
	db, err := sql.Open("mysql", dsn)

	if err != nil {
		return nil, err
	}

	c.db = db

	return &c, nil

}

func (c client) QueryToS3(query, path, separator, newLine string, encoding, contentType string, zip bool) error {

	csv, err := c.queryToCSV(query, separator, newLine, zip)

	if err != nil {
		return err
	}

	_, err = c.uploadS3(path, csv, encoding, contentType)

	return err
}

func (c client) queryToCSV(q string, separator, newLine string, zip bool) ([]byte, error) {
	var w io.Writer
	var buffer bytes.Buffer

	if zip {
		w = gzip.NewWriter(&buffer)

	} else {
		w = bufio.NewWriter(&buffer)

	}

	rows, err := c.db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	headers, err := rows.Columns()

	if err != nil {
		return nil, err
	}

	if len(headers) == 0 {
		return nil, errors.New("no headers")
	}

	lh := len(headers)

	for _, v := range headers[:lh-1] {
		io.WriteString(w, v)
		io.WriteString(w, separator)

	}

	io.WriteString(w, headers[lh-1])
	io.WriteString(w, newLine)

	nullRow := make([]sql.NullString, lh)
	aux := make([]interface{}, lh)

	for i := range nullRow {
		nullRow[i] = sql.NullString{}
		aux[i] = &nullRow[i]
	}

	for rows.Next() {

		err := rows.Scan(aux...)
		if err != nil {
			return nil, err
		}
		for _, v := range nullRow[:lh-1] {
			if v.Valid {
				io.WriteString(w, v.String)
			}
			io.WriteString(w, separator)

		}

		if nullRow[lh-1].Valid {
			io.WriteString(w, nullRow[lh-1].String)
		}

		io.WriteString(w, newLine)

	}

	if zip {
		w.(*gzip.Writer).Close()
	} else {
		w.(*bufio.Writer).Flush()
	}

	return buffer.Bytes(), nil
}

func (c client) Close() error {
	return c.db.Close()
}

func (c client) uploadS3(path string, bodyBytes []byte, contentEncoding, contentType string) (*s3.PutObjectOutput, error) {

	return s3.New(c.session).PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(c.bucket),
		Key:                  aws.String(path),
		ACL:                  aws.String("private"),
		Body:                 bytes.NewReader(bodyBytes),
		ContentLength:        aws.Int64(int64(len(bodyBytes))),
		ContentType:          aws.String(contentType),
		ContentEncoding:      aws.String(contentEncoding),
		ServerSideEncryption: aws.String("AES256"),
	})
}
