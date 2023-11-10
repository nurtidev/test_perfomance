#radgo

Что бы создать новый сервис:
1) Скопипастить соседний к примеру get-events/
2) Переименованть 
 - get-events в go.mod
 - get-events файл proto/get-events
 - get-events в файле proto/get-events заголовки на новое имя сервиса:
 - в its-service.go и main.go все импорты на import "new-service-name/its-service" итд
 - в main.go (49) изменить pb.RegisterGetEventsServer(s, &service.Service{}) на pb.RegisterNewServiceNameServer(s, &service.Service{})
 - в main.go (80) изменить pb.GetEvents_ServiceDesc.ServiceName на pb.NewServiceName_ServiceDesc.ServiceName
option go_package = "/new-service-name";
package new_service_name;
service NewServiceName {
    //...
}

3) Описать все методы которые какие нам надо в new-service-name.proto

3) В корне папки микросервиса (не в папке proto) выполнить:
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative .\proto\create-events.proto

4) В папке its-service/service.go переопределить класс Service и реализовать все методы которые описали в new-service-name.proto
type Service struct {
	pb.UnimplementedNewServiceNameServer
}

func (s *Service) CreateEvents(ctx context.Context, req *pb.Short) (*pb.GetEventsResponse, error) {
    //---
}

5) Выполнить корне `go mod tidy `

ну и потом `go build -o main.exe .`
... готова.