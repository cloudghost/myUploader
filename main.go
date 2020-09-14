package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var objectSuffixes = make(map[string]uint64)
var bucketName = ""

func initMinioClient(endpoint, accessKeyID, secretAccessKey string, useSSL bool) (*minio.Client, error) {
	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}
	return minioClient, nil
}


func makeBucket(minioClient *minio.Client, bucketName string) error {
	ctx := context.Background()
	// todo add location
	err := minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := minioClient.BucketExists(ctx, bucketName)
		if errBucketExists == nil && exists {
			return errors.New(fmt.Sprintf("We already own %s\n", bucketName))
		} else {
			return err
		}
	} else {
		log.Printf("Successfully created %s\n", bucketName)
	}
	return  nil
}
func fileUpload(minioClient *minio.Client, bucketName, objectName, filePath string) error {
	// Upload the file with FPutObject
	n, err := minioClient.FPutObject(context.Background(), bucketName, objectName, filePath, minio.PutObjectOptions{})
	if err != nil {
		return err
	}

	log.Printf("Successfully uploaded %s of size %d\n", objectName, n.Size)
	return nil
}

func readFile(minioClient *minio.Client, dstDir string) error {
	log.Println("readFile starts")
	files, err := ioutil.ReadDir(dstDir)
	if err != nil {
		return err
	}
	for _, f := range files {
		filePath := path.Join(dstDir, f.Name())
		file, err := os.OpenFile(path.Join(dstDir, f.Name()), os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		objectName := generateObjectName(f.Name())
		if err := fileUpload(minioClient, bucketName, objectName, filePath); err != nil {
			_ = file.Close()
			return err
		}
		if err = file.Truncate(0); err != nil {
			_ = file.Close()
			return err
		}
		if _, err := file.Seek(0, 0); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func generateObjectName(name string) string {
	prev := objectSuffixes[name]
	objectSuffixes[name] = prev + 1
	return name + "-" + strconv.FormatUint(prev, 10)
}

// todo too many args could extract some to global variable
func monitor(cmd *exec.Cmd, stopc chan int, client *minio.Client, dstDir string, limit int64) error {
	inc := time.After(1 * time.Second)
	//ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-stopc:
			log.Println("ending")
			if err := readFile(client, dstDir); err != nil {
				return err
			}
			//ticker.Stop()
			stopc<-1
			return nil
		case <- inc:
			//log.Println("inc")
			over, err := overLimit(dstDir, limit)
			if err != nil {
				return err
			}
			if over {
				if err := cmd.Process.Signal(syscall.SIGSTOP); err != nil {
					//todo clean created minio buckets
					return err
				}
				if err := readFile(client, dstDir); err != nil {
					return err
				}
				if err := cmd.Process.Signal(syscall.SIGCONT); err != nil {
					//todo clean created minio buckets
					return err
				}
			}
			inc = time.After(1 * time.Second)
		}
	}
}

func overLimit(dir string, limit int64) (bool, error) {
	if stat, err := os.Stat(dir); err != nil {
		return true, err
	} else if !stat.IsDir() {
		return true, errors.New(dir + " not a directory")
	}
	//todo not sure if mydumper will create sub dir under dstDir, could use walk to list files recursively
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return true, err
	}
	var size int64 = 0
	for _, f := range files {
		size += f.Size()
		if size >= limit {
			return true, nil
		}
	}
	return false, nil
}

func main() {
	dstDir := flag.String("o", "dump", "dump destination dir")
	other := flag.String("args", "", "args to mydumper")
	limit := flag.Int64("l", 0, "limit of disk usage in bytes by mydumper before a upload")
	flag.Parse()
	if *limit == 0 {
		log.Fatal("please set limit with -l")
	}
	if _, err := os.Stat(*dstDir); !os.IsNotExist(err) {
		log.Fatal("output dir already exists")
	}
	args := prepareArgs(*dstDir, *other)
	cmd := exec.Command("mydumper", args...)

	// set var to get the output
	var out bytes.Buffer

	// set the output to our variable
	cmd.Stderr = &out
	// todo add args to control endpoint, accessKeyID and secretAccessKey
	minioClient, err := initMinioClient("play.min.io", "Q3AM3UQ867SPQQA43P2F", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG", true)
	if err != nil {
		log.Fatal("fail to init minio client")
	}
	bucketName = time.Now().Format("20060102150405") + "-mydumper"
	if err := makeBucket(minioClient, bucketName); err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	mydumperFinish := make(chan int)
	go func() {
		err := monitor(cmd, mydumperFinish, minioClient, *dstDir, *limit)
		defer cmd.Process.Kill()
		defer os.RemoveAll(*dstDir)
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err = cmd.Wait(); err != nil {
		_ = os.RemoveAll(*dstDir)
		log.Fatal(err, out.String())
	}
	mydumperFinish <- 1
	<-mydumperFinish
	if err := os.RemoveAll(*dstDir); err != nil {
		log.Fatal(err)
	}
}

func prepareArgs(dst, other string) []string {
	otherArgs := strings.Split(other, "|")
	otherArgs = append(otherArgs, "-o", dst)
	return otherArgs
}
