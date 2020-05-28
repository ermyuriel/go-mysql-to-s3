package mys3

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"database/sql"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	_ "github.com/go-sql-driver/mysql"
)

type client struct {
	session  *session.Session
	db       *sql.DB
	region   string
	bucket   string
	uploader *s3manager.Uploader
}

func GetDSN(dbUser, dbPassword, dbHost, dbPort, dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPassword, dbHost, dbPort, dbName)
}

func (c *client) ConnectToDB(dbType, dsn string) error {

	db, err := sql.Open(dbType, dsn)
	if err != nil {
		return err
	}

	c.db = db

	return nil

}

func Client(region, bucket string) (*client, error) {

	c := client{region: region, bucket: bucket}

	s, err := session.NewSession(&aws.Config{Region: aws.String(region)})

	if err != nil {
		return nil, err
	}

	c.uploader = s3manager.NewUploader(s)

	c.session = s

	return &c, nil

}

func (c client) StringToS3(str, path string, encoding, contentType string, zip bool) error {
	var w io.Writer
	var buffer bytes.Buffer

	if zip {
		w = gzip.NewWriter(&buffer)

	} else {
		w = bufio.NewWriter(&buffer)

	}

	io.WriteString(w, str)

	if zip {
		w.(*gzip.Writer).Close()
	} else {
		w.(*bufio.Writer).Flush()
	}

	_, err := c.upload(path, buffer.Bytes(), encoding, contentType)

	return err
}

func (c client) QueryToS3(query, path, separator, newLine string, contentEncoding, contentType string, zip bool, skipEmpty bool) error {

	r, rc, err := c.queryToCSV(query, separator, newLine, zip)

	if rc == 0 && skipEmpty {
		return nil
	}

	_, err = c.uploader.Upload(&s3manager.UploadInput{
		Bucket:               aws.String(c.bucket),
		Key:                  aws.String(path),
		ACL:                  aws.String("private"),
		Body:                 r,
		ContentType:          aws.String(contentType),
		ContentEncoding:      aws.String(contentEncoding),
		ServerSideEncryption: aws.String("AES256"),
	})

	return err

}

func (c client) queryToCSV(q string, separator, newLine string, zip bool) (io.Reader, int, error) {
	var w io.Writer
	var buffer bytes.Buffer

	if zip {
		w = gzip.NewWriter(&buffer)
		defer w.(*gzip.Writer).Close()

	} else {
		w = bufio.NewWriter(&buffer)
		defer w.(*bufio.Writer).Flush()

	}

	rows, err := c.db.Query(q)
	if err != nil {
		return &buffer, 0, err
	}
	defer rows.Close()

	headers, err := rows.Columns()

	if err != nil {
		return &buffer, 0, err
	}

	lh := len(headers)

	for _, v := range headers[:lh-1] {
		_, err = io.WriteString(w, v)
		if err != nil {
			return &buffer, 0, err
		}
		_, err = io.WriteString(w, separator)
		if err != nil {
			return &buffer, 0, err
		}

	}

	_, err = io.WriteString(w, headers[lh-1])
	if err != nil {
		return &buffer, 0, err
	}
	_, err = io.WriteString(w, newLine)
	if err != nil {
		return &buffer, 0, err
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
			return &buffer, rc, err
		}
		for _, v := range nullRow[:lh-1] {
			if v.Valid {
				_, err = io.WriteString(w, v.String)
				if err != nil {
					return &buffer, rc, err
				}
			}
			_, err = io.WriteString(w, separator)
			if err != nil {
				return &buffer, rc, err
			}

		}

		if nullRow[lh-1].Valid {
			_, err = io.WriteString(w, nullRow[lh-1].String)
			if err != nil {
				return &buffer, rc, err
			}
		}

		_, err = io.WriteString(w, newLine)
		if err != nil {
			return &buffer, rc, err
		}

	}

	return &buffer, rc, err

}

func (c *client) Close() error {
	return c.db.Close()
}

func (c client) upload(path string, bodyBytes []byte, contentEncoding, contentType string) (*s3.PutObjectOutput, error) {

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
