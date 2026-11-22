package call_service

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	whatsmeow_service "github.com/evolution-foundation/evolution-go/pkg/whatsmeow/service"
	"github.com/gomessguii/logger"
	"github.com/gorilla/websocket"
	meowcaller "github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

type ActiveCallInfo struct {
	InstanceID string
	Call       *meowcaller.Call
	CreatedAt  time.Time
}

var (
	activeCallsMu sync.RWMutex
	activeCalls   = make(map[string]*ActiveCallInfo)
)

func RegisterActiveCall(instanceId string, call *meowcaller.Call) {
	if call == nil {
		return
	}
	activeCallsMu.Lock()
	activeCalls[call.ID()] = &ActiveCallInfo{
		InstanceID: instanceId,
		Call:       call,
		CreatedAt:  time.Now(),
	}
	activeCallsMu.Unlock()

	call.OnEnd(func(reason string) {
		activeCallsMu.Lock()
		delete(activeCalls, call.ID())
		activeCallsMu.Unlock()
	})
}

func GetActiveCall(callID string) (*ActiveCallInfo, bool) {
	activeCallsMu.RLock()
	defer activeCallsMu.RUnlock()
	info, ok := activeCalls[callID]
	return info, ok
}

type CallService interface {
	RejectCall(data *RejectCallStruct, instance *instance_model.Instance) error
	AnswerCall(callId string, instance *instance_model.Instance) error
	OfferCall(data *OfferCallStruct, instance *instance_model.Instance) (*OfferCallResponse, error)
	PlayAudio(data *PlayAudioStruct, instance *instance_model.Instance) error
	HandleAudioStream(callId string, instance *instance_model.Instance, ws *websocket.Conn) error
}

type callService struct {
	clientPointer    map[string]*whatsmeow.Client
	callerPointer    map[string]*meowcaller.Client
	whatsmeowService whatsmeow_service.WhatsmeowService
	loggerWrapper    *logger_wrapper.LoggerManager
}

type RejectCallStruct struct {
	CallCreator types.JID `json:"callCreator"`
	CallID      string    `json:"callId"`
}

type AnswerCallStruct struct {
	CallID string `json:"callId"`
}

type OfferCallStruct struct {
	Number string `json:"number"`
	Video  bool   `json:"video,omitempty"`
}

type OfferCallResponse struct {
	CallID string `json:"callId"`
	Peer   string `json:"peer"`
	Status string `json:"status"`
}

type PlayAudioStruct struct {
	CallID    string `json:"callId"`
	AudioPath string `json:"audioPath"`
}

func (c *callService) ensureClientConnected(instanceId string) (*whatsmeow.Client, error) {
	client := c.clientPointer[instanceId]
	c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Checking client connection status - Client exists: %v", instanceId, client != nil)

	if client == nil {
		c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] No client found, attempting to start new instance", instanceId)
		err := c.whatsmeowService.StartInstance(instanceId)
		if err != nil {
			c.loggerWrapper.GetLogger(instanceId).LogError("[%s] Failed to start instance: %v", instanceId, err)
			return nil, errors.New("no active session found")
		}

		c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Instance started, waiting 2 seconds...", instanceId)
		time.Sleep(2 * time.Second)

		client = c.clientPointer[instanceId]
		c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Checking new client - Exists: %v, Connected: %v",
			instanceId,
			client != nil,
			client != nil && client.IsConnected())

		if client == nil || !client.IsConnected() {
			c.loggerWrapper.GetLogger(instanceId).LogError("[%s] New client validation failed - Exists: %v, Connected: %v",
				instanceId,
				client != nil,
				client != nil && client.IsConnected())
			return nil, errors.New("no active session found")
		}
	} else if !client.IsConnected() {
		c.loggerWrapper.GetLogger(instanceId).LogError("[%s] Existing client is disconnected - Connected status: %v",
			instanceId,
			client.IsConnected())
		return nil, errors.New("client disconnected")
	}

	c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Client successfully validated - Connected: %v", instanceId, client.IsConnected())
	return client, nil
}

func (c *callService) RejectCall(data *RejectCallStruct, instance *instance_model.Instance) error {
	if info, ok := GetActiveCall(data.CallID); ok && info.Call != nil {
		_ = info.Call.Reject()
	}

	client, err := c.ensureClientConnected(instance.Id)
	if err != nil {
		return err
	}

	err = client.RejectCall(context.Background(), data.CallCreator, data.CallID)
	if err != nil {
		logger.LogError("[%s] error reject call: %v", instance.Id, err)
		return err
	}

	return nil
}

func (c *callService) AnswerCall(callId string, instance *instance_model.Instance) error {
	info, ok := GetActiveCall(callId)
	if !ok || info.Call == nil {
		return errors.New("chamada não encontrada ou já encerrada")
	}
	return info.Call.Answer()
}

func (c *callService) OfferCall(data *OfferCallStruct, instance *instance_model.Instance) (*OfferCallResponse, error) {
	caller := c.callerPointer[instance.Id]
	if caller == nil {
		return nil, errors.New("caller client não inicializado para esta instância")
	}

	target := strings.TrimSpace(data.Number)
	target = strings.TrimPrefix(target, "+")

	// Se não contiver '@', é um número puro: normaliza e tenta resolver LID/JID canônico
	if !strings.ContainsRune(target, '@') {
		digitsOnly := ""
		for _, r := range target {
			if r >= '0' && r <= '9' {
				digitsOnly += string(r)
			}
		}
		if (len(digitsOnly) == 10 || len(digitsOnly) == 11) && !strings.HasPrefix(digitsOnly, "55") {
			digitsOnly = "55" + digitsOnly
		}
		if len(digitsOnly) >= 10 {
			target = digitsOnly
		}

		client := c.clientPointer[instance.Id]
		if client != nil {
			if resp, err := client.IsOnWhatsApp(context.Background(), []string{digitsOnly}); err == nil && len(resp) > 0 && resp[0].IsIn {
				if client.Store != nil && client.Store.LIDs != nil {
					if lid, err := client.Store.LIDs.GetLIDForPN(context.Background(), resp[0].JID); err == nil && !lid.IsEmpty() {
						target = lid.String()
					}
				}
				if !strings.HasSuffix(target, "@lid") {
					target = resp[0].JID.String()
				}
			}
		}
	} else if strings.HasSuffix(target, "@s.whatsapp.net") {
		// Se vier formatado com @s.whatsapp.net, verifica se já temos o LID mapeado
		client := c.clientPointer[instance.Id]
		if client != nil && client.Store != nil && client.Store.LIDs != nil {
			if jid, err := types.ParseJID(target); err == nil {
				if lid, err := client.Store.LIDs.GetLIDForPN(context.Background(), jid); err == nil && !lid.IsEmpty() {
					target = lid.String()
				}
			}
		}
	}

	call, err := caller.CallWithOptions(context.Background(), target, meowcaller.CallOptions{Video: data.Video})
	if err != nil {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Erro ao originar chamada para %s: %v", instance.Id, target, err)
		return nil, err
	}

	RegisterActiveCall(instance.Id, call)

	return &OfferCallResponse{
		CallID: call.ID(),
		Peer:   call.Peer().String(),
		Status: "calling",
	}, nil
}

func (c *callService) PlayAudio(data *PlayAudioStruct, instance *instance_model.Instance) error {
	info, ok := GetActiveCall(data.CallID)
	if !ok || info.Call == nil {
		return errors.New("chamada não encontrada ou já encerrada")
	}

	var src meowcaller.AudioSource
	var err error
	lower := strings.ToLower(data.AudioPath)
	if strings.HasSuffix(lower, ".mp3") {
		src, err = meowcaller.MP3File(data.AudioPath)
	} else if strings.HasSuffix(lower, ".wav") {
		src, err = meowcaller.WAVFile(data.AudioPath)
	} else if strings.HasSuffix(lower, ".opus") {
		src, err = meowcaller.OpusFile(data.AudioPath)
	} else {
		return errors.New("formato de áudio não suportado (use .mp3, .wav ou .opus)")
	}

	if err != nil {
		return err
	}

	info.Call.Play(src)
	return nil
}

func (c *callService) HandleAudioStream(callId string, instance *instance_model.Instance, ws *websocket.Conn) error {
	info, ok := GetActiveCall(callId)
	if !ok || info.Call == nil {
		return errors.New("chamada não encontrada ou já encerrada")
	}
	call := info.Call

	pr, pw := io.Pipe()
	defer pw.Close()
	defer pr.Close()

	// 1. Injeta áudio do microfone do WebSocket na chamada
	call.Play(meowcaller.PCMStream(pr))

	// 2. Recebe áudio do WhatsApp e transmite via WebSocket como s16le PCM (16kHz)
	var writeMu sync.Mutex
	call.Receive(meowcaller.SinkFunc(func(frame []float32) {
		buf := make([]byte, len(frame)*2)
		for i, s := range frame {
			v := s * 32768.0
			if v > 32767 {
				v = 32767
			} else if v < -32768 {
				v = -32768
			}
			binary.LittleEndian.PutUint16(buf[2*i:], uint16(int16(v)))
		}

		writeMu.Lock()
		_ = ws.WriteMessage(websocket.BinaryMessage, buf)
		writeMu.Unlock()
	}))

	// 3. Lê pacotes de áudio enviados pelo cliente no WebSocket e joga no Pipe
	for {
		msgType, data, err := ws.ReadMessage()
		if err != nil {
			break
		}
		if msgType == websocket.BinaryMessage && len(data) > 0 {
			_, err = pw.Write(data)
			if err != nil {
				break
			}
		}
	}

	return nil
}

func NewCallService(
	clientPointer map[string]*whatsmeow.Client,
	callerPointer map[string]*meowcaller.Client,
	whatsmeowService whatsmeow_service.WhatsmeowService,
	loggerWrapper *logger_wrapper.LoggerManager,
) CallService {
	return &callService{
		clientPointer:    clientPointer,
		callerPointer:    callerPointer,
		whatsmeowService: whatsmeowService,
		loggerWrapper:    loggerWrapper,
	}
}
