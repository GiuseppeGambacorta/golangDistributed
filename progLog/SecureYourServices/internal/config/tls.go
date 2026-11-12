package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	Server        bool
}

func SetupTLSConfig(config TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: config.ServerAddress,
	}

	if config.CertFile != "" && config.KeyFile != "" {
		certificate, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	if config.CAFile == "" {
		return tlsConfig, nil
	}
	certificatePEM, err := os.ReadFile(config.CAFile)
	if err != nil {
		return nil, err
	}
	certificatePool := x509.NewCertPool()
	if !certificatePool.AppendCertsFromPEM(certificatePEM) {
		return nil, fmt.Errorf("failed to parse root certificate: %q", config.CAFile)
	}

	if config.Server {
		tlsConfig.ClientCAs = certificatePool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		tlsConfig.RootCAs = certificatePool
	}
	return tlsConfig, nil
}
