(async() => {
 const panel=document.querySelector('[data-push-panel]');
 if(!panel)return;
 const status=panel.querySelector('[data-push-feedback]');
 const enable=panel.querySelector('[data-enable-push]');
 const disable=panel.querySelector('[data-disable-push]');
 const csrf=panel.dataset.csrf;
 const say=message=>status.textContent=message;
 const supported=window.isSecureContext && 'serviceWorker' in navigator && 'PushManager' in window && 'Notification' in window;
 if(!supported){say('Ce navigateur ne prend pas en charge les notifications Push sécurisées.');return;}
 const registration=await navigator.serviceWorker.ready;
 const key=panel.dataset.publicKey;
 const bytes=b64=>{const input=(b64+'='.repeat((4-b64.length%4)%4)).replace(/-/g,'+').replace(/_/g,'/');return Uint8Array.from(atob(input),c=>c.charCodeAt(0));};
 const send=async(path,data)=>{
    const params=new URLSearchParams({...data,csrf});
    const r=await fetch(path,{method:'POST',credentials:'same-origin',body:params});
    if(!r.ok)throw new Error((await r.json().catch(()=>({}))).error || 'Erreur de connexion ('+r.status+')');
    return r.json();
 };
 enable.addEventListener('click',async()=>{
   try{
     const permission=await Notification.requestPermission();
     if(permission!=='granted'){say('Autorisation refusée dans les réglages du téléphone.');return;}
     let subscription=await registration.pushManager.getSubscription();
     if(!subscription) subscription=await registration.pushManager.subscribe({userVisibleOnly:true,applicationServerKey:bytes(key)});
     await send('/push/subscribe',{subscription:JSON.stringify(subscription.toJSON())});
     say('✅ Notifications activées sur cet appareil.');
   }catch(error){say(error.message);}
 });
 disable.addEventListener('click',async()=>{
   try{
     const subscription=await registration.pushManager.getSubscription();
     if(subscription){await send('/push/unsubscribe',{endpoint:subscription.endpoint});await subscription.unsubscribe();}
     say('Notifications désactivées pour cet appareil.');
   }catch(error){say(error.message);}
 });
})().catch(console.error);
