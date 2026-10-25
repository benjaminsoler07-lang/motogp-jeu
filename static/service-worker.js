self.addEventListener('push', event => {
  let message={title:'PronoGP',body:'Nouveau message',url:'/notifications'};
  if(event.data){try{message={...message,...event.data.json()}}catch(e){message.body=event.data.text()}}
  const path=typeof message.url==='string' && message.url.startsWith('/') && !message.url.startsWith('//') ? message.url : '/notifications';
  event.waitUntil(self.registration.showNotification(message.title,{
    body:message.body,icon:'/pwa/icon/192',badge:'/pwa/icon/192',data:{url:path}
  }));
});
self.addEventListener('notificationclick', event => {
  event.notification.close();
  const path=event.notification.data && event.notification.data.url || '/notifications';
  event.waitUntil(clients.openWindow(new URL(path,self.location.origin).href));
});
