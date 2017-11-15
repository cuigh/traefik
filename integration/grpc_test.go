package integration

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"net"
	"os"
	"time"

	"github.com/containous/traefik/integration/helloworld"
	"github.com/containous/traefik/integration/try"
	"github.com/go-check/check"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var LocalhostCert []byte
var LocalhostKey []byte

// GRPCSuite
type GRPCSuite struct{ BaseSuite }

type myserver struct {
	stopStreamExample chan bool
}

func (s *GRPCSuite) SetUpSuite(c *check.C) {
	var err error
	LocalhostCert, err = ioutil.ReadFile("./resources/tls/local.cert")
	c.Assert(err, check.IsNil)
	LocalhostKey, err = ioutil.ReadFile("./resources/tls/local.key")
	c.Assert(err, check.IsNil)
}

func (s *myserver) SayHello(ctx context.Context, in *helloworld.HelloRequest) (*helloworld.HelloReply, error) {
	return &helloworld.HelloReply{Message: "Hello " + in.Name}, nil
}

func (s *myserver) StreamExample(in *helloworld.StreamExampleRequest, server helloworld.Greeter_StreamExampleServer) error {
	data := make([]byte, 512)
	rand.Read(data)
	server.Send(&helloworld.StreamExampleReply{Data: string(data)})
	<-s.stopStreamExample
	return nil
}

func startGRPCServer(lis net.Listener, server *myserver) error {
	cert, err := tls.X509KeyPair(LocalhostCert, LocalhostKey)
	if err != nil {
		return err
	}

	creds := credentials.NewServerTLSFromCert(&cert)
	serverOption := grpc.Creds(creds)

	s := grpc.NewServer(serverOption)
	defer s.Stop()

	helloworld.RegisterGreeterServer(s, server)
	return s.Serve(lis)
}
func getHelloClientGRPC() (helloworld.GreeterClient, func() error, error) {
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(LocalhostCert)
	credsClient := credentials.NewClientTLSFromCert(roots, "")
	conn, err := grpc.Dial("127.0.0.1:4443", grpc.WithTransportCredentials(credsClient))
	if err != nil {
		return nil, func() error { return nil }, err
	}
	return helloworld.NewGreeterClient(conn), conn.Close, nil

}

func callHelloClientGRPC(name string) (string, error) {
	client, closer, err := getHelloClientGRPC()
	defer closer()
	if err != nil {
		return "", err
	}
	r, err := client.SayHello(context.Background(), &helloworld.HelloRequest{Name: name})
	if err != nil {
		return "", err
	}
	return r.Message, nil
}

func callStreamExampleClientGRPC() (helloworld.Greeter_StreamExampleClient, func() error, error) {
	client, closer, err := getHelloClientGRPC()
	if err != nil {
		return nil, closer, err
	}
	t, err := client.StreamExample(context.Background(), &helloworld.StreamExampleRequest{})
	if err != nil {
		return nil, closer, err
	}

	return t, closer, nil
}

func (s *GRPCSuite) TestGRPC(c *check.C) {
	lis, err := net.Listen("tcp", ":0")
	_, port, err := net.SplitHostPort(lis.Addr().String())
	c.Assert(err, check.IsNil)

	go func() {
		err := startGRPCServer(lis, &myserver{})
		c.Log(err)
		c.Assert(err, check.IsNil)
	}()

	file := s.adaptFile(c, "fixtures/grpc/config.toml", struct {
		CertContent    string
		KeyContent     string
		GRPCServerPort string
	}{
		CertContent:    string(LocalhostCert),
		KeyContent:     string(LocalhostKey),
		GRPCServerPort: port,
	})

	defer os.Remove(file)
	cmd, display := s.traefikCmd(withConfigFile(file))
	defer display(c)

	err = cmd.Start()
	c.Assert(err, check.IsNil)
	defer cmd.Process.Kill()

	// wait for Traefik
	err = try.GetRequest("http://127.0.0.1:8080/api/providers", 1*time.Second, try.BodyContains("Host:127.0.0.1"))
	c.Assert(err, check.IsNil)
	var response string
	err = try.Do(1*time.Second, func() error {
		response, err = callHelloClientGRPC("World")
		return err
	})

	c.Assert(err, check.IsNil)
	c.Assert(response, check.Equals, "Hello World")
}

func (s *GRPCSuite) TestGRPCInsecure(c *check.C) {
	lis, err := net.Listen("tcp", ":0")
	_, port, err := net.SplitHostPort(lis.Addr().String())
	c.Assert(err, check.IsNil)

	go func() {
		err := startGRPCServer(lis, &myserver{})
		c.Log(err)
		c.Assert(err, check.IsNil)
	}()

	file := s.adaptFile(c, "fixtures/grpc/config_insecure.toml", struct {
		CertContent    string
		KeyContent     string
		GRPCServerPort string
	}{
		CertContent:    string(LocalhostCert),
		KeyContent:     string(LocalhostKey),
		GRPCServerPort: port,
	})

	defer os.Remove(file)
	cmd, display := s.traefikCmd(withConfigFile(file))
	defer display(c)

	err = cmd.Start()
	c.Assert(err, check.IsNil)
	defer cmd.Process.Kill()

	// wait for Traefik
	err = try.GetRequest("http://127.0.0.1:8080/api/providers", 1*time.Second, try.BodyContains("Host:127.0.0.1"))
	c.Assert(err, check.IsNil)
	var response string
	err = try.Do(1*time.Second, func() error {
		response, err = callHelloClientGRPC("World")
		return err
	})

	c.Assert(err, check.IsNil)
	c.Assert(response, check.Equals, "Hello World")
}

func (s *GRPCSuite) TestGRPCBuffer(c *check.C) {
	stopStreamExample := make(chan bool)
	defer func() { stopStreamExample <- true }()
	lis, err := net.Listen("tcp", ":0")
	_, port, err := net.SplitHostPort(lis.Addr().String())
	c.Assert(err, check.IsNil)

	go func() {
		err := startGRPCServer(lis, &myserver{
			stopStreamExample: stopStreamExample,
		})
		c.Log(err)
		c.Assert(err, check.IsNil)
	}()

	file := s.adaptFile(c, "fixtures/grpc/config.toml", struct {
		CertContent    string
		KeyContent     string
		GRPCServerPort string
	}{
		CertContent:    string(LocalhostCert),
		KeyContent:     string(LocalhostKey),
		GRPCServerPort: port,
	})

	defer os.Remove(file)
	cmd, display := s.traefikCmd(withConfigFile(file))
	defer display(c)

	err = cmd.Start()
	c.Assert(err, check.IsNil)
	defer cmd.Process.Kill()

	// wait for Traefik
	err = try.GetRequest("http://127.0.0.1:8080/api/providers", 1*time.Second, try.BodyContains("Host:127.0.0.1"))
	c.Assert(err, check.IsNil)
	var client helloworld.Greeter_StreamExampleClient
	client, closer, err := callStreamExampleClientGRPC()
	defer closer()

	received := make(chan bool)
	go func() {
		tr, _ := client.Recv()
		c.Assert(len(tr.Data), check.Equals, 512)
		received <- true
	}()

	err = try.Do(time.Second*10, func() error {
		select {
		case <-received:
			return nil
		default:
			return errors.New("failed to receive stream data")
		}
	})
	c.Assert(err, check.IsNil)
}
