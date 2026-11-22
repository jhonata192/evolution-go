package call_handler

import (
	"net/http"

	call_service "github.com/evolution-foundation/evolution-go/pkg/call/service"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type CallHandler interface {
	RejectCall(ctx *gin.Context)
	AnswerCall(ctx *gin.Context)
	OfferCall(ctx *gin.Context)
	PlayAudio(ctx *gin.Context)
	StreamCall(ctx *gin.Context)
}

type callHandler struct {
	callService call_service.CallService
}

// Reject call
func (g *callHandler) RejectCall(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")
	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *call_service.RejectCallStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err = g.callService.RejectCall(data, instance)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success"})
}

// Answer incoming call
func (g *callHandler) AnswerCall(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")
	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data call_service.AnswerCallStruct
	err := ctx.ShouldBindJSON(&data)
	if err != nil || data.CallID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "callId é obrigatório"})
		return
	}

	err = g.callService.AnswerCall(data.CallID, instance)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success", "callId": data.CallID})
}

// Offer outbound call
func (g *callHandler) OfferCall(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")
	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data call_service.OfferCallStruct
	err := ctx.ShouldBindJSON(&data)
	if err != nil || data.Number == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "number é obrigatório"})
		return
	}

	resp, err := g.callService.OfferCall(&data, instance)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, resp)
}

// Play audio into call
func (g *callHandler) PlayAudio(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")
	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data call_service.PlayAudioStruct
	err := ctx.ShouldBindJSON(&data)
	if err != nil || data.CallID == "" || data.AudioPath == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "callId e audioPath são obrigatórios"})
		return
	}

	err = g.callService.PlayAudio(&data, instance)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "audio playing", "callId": data.CallID})
}

// StreamCall upgrades to WebSocket for bidirectional audio
func (g *callHandler) StreamCall(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")
	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	callId := ctx.Param("callId")
	if callId == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "callId é obrigatório"})
		return
	}

	ws, err := wsUpgrader.Upgrade(ctx.Writer, ctx.Request, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	_ = g.callService.HandleAudioStream(callId, instance, ws)
}

func NewCallHandler(
	callService call_service.CallService,
) CallHandler {
	return &callHandler{
		callService: callService,
	}
}
